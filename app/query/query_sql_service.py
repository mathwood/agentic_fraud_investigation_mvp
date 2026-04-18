from __future__ import annotations

import sqlite3
from typing import Any

from app.query.query_contract import ActionFilter, DatasetQuery, QueryIntent
from app.storage.dataset_store import get_connection


class QuerySQLServiceError(ValueError):
    pass


def _build_where_clause(query: DatasetQuery) -> tuple[str, list]:
    filters = query.filters
    clauses: list[str] = []
    params: list[Any] = []

    if filters.user_id is not None:
        clauses.append("user_id = ?")
        params.append(filters.user_id)

    if filters.merchant_category is not None:
        clauses.append("LOWER(merchant_category) = LOWER(?)")
        params.append(filters.merchant_category)

    if filters.country is not None:
        clauses.append("LOWER(country) = LOWER(?)")
        params.append(filters.country)

    if filters.bin_country is not None:
        clauses.append("LOWER(bin_country) = LOWER(?)")
        params.append(filters.bin_country)

    if filters.is_fraud is not None:
        clauses.append("is_fraud = ?")
        params.append(1 if filters.is_fraud else 0)

    if filters.recommended_action is not None:
        if filters.recommended_action == ActionFilter.SUSPICIOUS:
            clauses.append("recommended_action IN ('manual_review', 'decline')")
        else:
            clauses.append("recommended_action = ?")
            params.append(filters.recommended_action.value)

    if not clauses:
        return "", params

    return "WHERE " + " AND ".join(clauses), params


def run_structured_query(query: DatasetQuery) -> dict[str, Any]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    where_clause, params = _build_where_clause(query)

    if query.intent == QueryIntent.NTH_HIGHEST_RISK_TRANSACTION:
        rank = query.rank or 1

        count_sql = f"SELECT COUNT(*) AS count FROM enriched_transactions {where_clause}"
        matched_count = cur.execute(count_sql, params).fetchone()["count"]

        if matched_count == 0:
            conn.close()
            raise QuerySQLServiceError(
                "No transactions are available in enriched_transactions for this query."
            )

        if rank > matched_count:
            conn.close()
            raise QuerySQLServiceError(
                f"Requested rank {rank}, but only {matched_count} transaction(s) are currently available."
            )

        sql = f"""
            SELECT *
            FROM enriched_transactions
            {where_clause}
            ORDER BY risk_score DESC,
                     CASE recommended_action
                        WHEN 'decline' THEN 3
                        WHEN 'manual_review' THEN 2
                        WHEN 'approve' THEN 1
                        ELSE 0
                     END DESC,
                     dataset_index ASC
            LIMIT 1 OFFSET ?
        """
        row = cur.execute(sql, params + [rank - 1]).fetchone()
        result = dict(row)
        conn.close()
        return {
            "result_type": "single_transaction",
            "rank": rank,
            "transaction": result,
        }

    if query.intent == QueryIntent.TOP_K_HIGHEST_RISK_TRANSACTIONS:
        limit = query.limit or 5
        sql = f"""
            SELECT *
            FROM enriched_transactions
            {where_clause}
            ORDER BY risk_score DESC,
                     CASE recommended_action
                        WHEN 'decline' THEN 3
                        WHEN 'manual_review' THEN 2
                        WHEN 'approve' THEN 1
                        ELSE 0
                     END DESC,
                     dataset_index ASC
            LIMIT ?
        """
        rows = [dict(r) for r in cur.execute(sql, params + [limit]).fetchall()]
        conn.close()
        return {
            "result_type": "transaction_list",
            "limit": limit,
            "transactions": rows,
        }

    if query.intent == QueryIntent.USER_WITH_MOST_SUSPICIOUS_TRANSACTIONS:
        sql = """
            SELECT
                user_id,
                COUNT(*) AS suspicious_transaction_count,
                SUM(CASE WHEN recommended_action = 'decline' THEN 1 ELSE 0 END) AS declined_transaction_count,
                AVG(risk_score) AS average_risk_score,
                MAX(risk_score) AS max_risk_score
            FROM enriched_transactions
            WHERE recommended_action IN ('manual_review', 'decline')
            GROUP BY user_id
            ORDER BY suspicious_transaction_count DESC,
                     declined_transaction_count DESC,
                     average_risk_score DESC,
                     user_id ASC
            LIMIT 1
        """
        row = cur.execute(sql).fetchone()
        if not row:
            conn.close()
            raise QuerySQLServiceError("No suspicious user found.")
        conn.close()
        return {
            "result_type": "single_user",
            "user": dict(row),
        }

    if query.intent == QueryIntent.TOP_K_USERS_BY_SUSPICIOUS_TRANSACTION_COUNT:
        limit = query.limit or 5
        sql = """
            SELECT
                user_id,
                COUNT(*) AS suspicious_transaction_count,
                SUM(CASE WHEN recommended_action = 'decline' THEN 1 ELSE 0 END) AS declined_transaction_count,
                AVG(risk_score) AS average_risk_score,
                MAX(risk_score) AS max_risk_score
            FROM enriched_transactions
            WHERE recommended_action IN ('manual_review', 'decline')
            GROUP BY user_id
            ORDER BY suspicious_transaction_count DESC,
                     declined_transaction_count DESC,
                     average_risk_score DESC,
                     user_id ASC
            LIMIT ?
        """
        rows = [dict(r) for r in cur.execute(sql, [limit]).fetchall()]
        conn.close()
        return {
            "result_type": "user_list",
            "limit": limit,
            "users": rows,
        }

    if query.intent == QueryIntent.COUNT_TRANSACTIONS:
        sql = f"SELECT COUNT(*) AS count FROM enriched_transactions {where_clause}"
        row = cur.execute(sql, params).fetchone()
        conn.close()
        return {
            "result_type": "count",
            "count": row["count"],
            "applied_filters": query.filters.model_dump(),
        }

    if query.intent == QueryIntent.TOP_CATEGORY_BY_TRANSACTION_COUNT:
        if query.group_by is None:
            conn.close()
            raise QuerySQLServiceError("group_by is required for category aggregation queries.")

        direction = "DESC" if (query.sort_direction is None or query.sort_direction.value == "desc") else "ASC"
        group_field = query.group_by.value
        limit = query.limit or 1

        sql = f"""
            SELECT
                {group_field} AS category_value,
                COUNT(*) AS transaction_count
            FROM enriched_transactions
            {where_clause}
            GROUP BY {group_field}
            ORDER BY transaction_count {direction}, category_value ASC
            LIMIT ?
        """

        rows = [dict(r) for r in cur.execute(sql, params + [limit]).fetchall()]
        conn.close()

        if not rows:
            raise QuerySQLServiceError("No grouped result found for this query.")

        if limit == 1:
            return {
                "result_type": "single_category",
                "group_by": group_field,
                "category": rows[0],
            }

        return {
            "result_type": "category_list",
            "group_by": group_field,
            "categories": rows,
            "limit": limit,
        }

    conn.close()
    raise QuerySQLServiceError(f"Unsupported query intent: {query.intent}")