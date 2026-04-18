import logging

from rich.console import Console
import typer

from app.config import get_settings
from app.main import (
    test_case_model,
    test_history_model,
    test_signal_models,
    test_report_model,
    test_case_loader,
    test_history_loader,
    test_velocity_tool,
    test_anomaly_tool,
    test_scoring_tool,
    test_llm_reporter,
    test_orchestrator,
    test_list_saved_reports,
    test_dataset_mapper,
    test_case_generator_from_dataset,
    test_dataset_orchestrator,
    find_first_fraud_row,
    find_first_normal_row,
    inspect_dataset,
)
from app.query.enrichment_builder import build_enriched_dataset
from app.query.query_sql_service import run_structured_query
from app.storage.dataset_store import (
    ingest_raw_dataset, 
    reset_dataset_tables,
    init_dataset_tables,
)
from app.query.query_llm_parser import parse_question_with_llm, QueryLLMParserError

app = typer.Typer()
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@app.command()
def start() -> None:
    settings = get_settings()
    console.print("[bold green]Fraud Investigation MVP booted successfully[/bold green]")
    console.print(f"Model: {settings.openai_model}")
    console.print(f"Data dir: {settings.data_dir}")
    console.print(f"DB path: {settings.db_path}")


@app.command()
def validate_case() -> None:
    test_case_model()


@app.command()
def validate_history() -> None:
    test_history_model()


@app.command()
def validate_signals() -> None:
    test_signal_models()


@app.command()
def validate_report() -> None:
    test_report_model()


@app.command()
def load_case(case_id: str = "CASE-1001") -> None:
    test_case_loader(case_id)


@app.command()
def load_history(customer_id: str = "CUST-781", lookback_days: int = 30) -> None:
    test_history_loader(customer_id, lookback_days)


@app.command()
def check_velocity_cmd() -> None:
    test_velocity_tool()


@app.command()
def check_anomalies_cmd() -> None:
    test_anomaly_tool()


@app.command()
def score_case_cmd() -> None:
    test_scoring_tool()


@app.command()
def generate_report_cmd() -> None:
    test_llm_reporter()


@app.command()
def investigate(case_id: str = "CASE-1001") -> None:
    test_orchestrator(case_id)


@app.command()
def list_reports() -> None:
    test_list_saved_reports()


@app.command()
def inspect_dataset_cmd(file_path: str) -> None:
    inspect_dataset(file_path)


@app.command()
def map_dataset_row_cmd(file_path: str) -> None:
    test_dataset_mapper(file_path)


@app.command()
def generate_case_from_dataset_cmd(file_path: str) -> None:
    test_case_generator_from_dataset(file_path)


@app.command()
def investigate_from_dataset(
    file_path: str = typer.Option(..., help="Path to dataset CSV file"),
    row_index: int = typer.Option(0, help="Row index to investigate"),
) -> None:
    test_dataset_orchestrator(file_path, row_index)


@app.command()
def first_fraud_row(file_path: str) -> None:
    find_first_fraud_row(file_path)


@app.command()
def first_normal_row(file_path: str) -> None:
    find_first_normal_row(file_path)

@app.command()
def reset_dataset_db() -> None:

    reset_dataset_tables()
    init_dataset_tables()

    console.print("[bold yellow]Dataset tables dropped and recreated[/bold yellow]")

@app.command()
def init_dataset_db() -> None:
    init_dataset_tables()
    console.print("[bold green]Dataset tables initialized[/bold green]")


@app.command()
def ingest_dataset(
    file_path: str = typer.Option(..., help="Path to dataset CSV file"),
) -> None:
    count = ingest_raw_dataset(file_path)
    console.print(f"[bold green]Ingested {count} raw transactions[/bold green]")


@app.command()
def build_enriched_dataset_cmd(
    log_every: int = typer.Option(500, help="Progress log frequency"),
    batch_size: int = typer.Option(1000, help="How many rows to commit per batch"),
    limit: int = typer.Option(0, help="Optional limit for testing. Use 0 for full dataset"),
) -> None:
    effective_limit = None if limit == 0 else limit
    count = build_enriched_dataset(
        log_every=log_every,
        batch_size=batch_size,
        limit=effective_limit,
    )
    console.print(f"[bold green]Built {count} enriched transactions[/bold green]")

def print_dataset_query_response(question: str, structured_query: dict, result: dict) -> None:
    console.print("[bold green]Dataset query executed successfully[/bold green]\n")

    console.print("[bold cyan]Question[/bold cyan]")
    console.print(question)
    console.print()

    console.print("[bold cyan]Structured Query[/bold cyan]")
    console.print(structured_query)
    console.print()

    result_type = result.get("result_type")

    if result_type == "single_transaction":
        tx = result["transaction"]
        console.print("[bold cyan]Answer[/bold cyan]")
        console.print(f"Index {tx['dataset_index']}")
        console.print(f"Transaction ID: {tx['transaction_id']}")
        console.print(f"User ID: {tx['user_id']}")
        console.print(f"Risk Score: {tx['risk_score']}")
        console.print(f"Recommended Action: {tx['recommended_action']}")
        return

    if result_type == "transaction_list":
        console.print("[bold cyan]Answer[/bold cyan]")
        for idx, tx in enumerate(result["transactions"], start=1):
            console.print(
                f"{idx}. Index {tx['dataset_index']} | "
                f"Transaction ID {tx['transaction_id']} | "
                f"User {tx['user_id']} | "
                f"Score {tx['risk_score']} | "
                f"Action {tx['recommended_action']}"
            )
        return

    if result_type == "single_user":
        user = result["user"]
        console.print("[bold cyan]Answer[/bold cyan]")
        console.print(f"User ID {user['user_id']}")
        console.print(f"Suspicious Transactions: {user['suspicious_transaction_count']}")
        console.print(f"Declined Transactions: {user['declined_transaction_count']}")
        console.print(f"Average Risk Score: {user['average_risk_score']:.2f}")
        console.print(f"Max Risk Score: {user['max_risk_score']:.2f}")
        return

    if result_type == "user_list":
        console.print("[bold cyan]Answer[/bold cyan]")
        for idx, user in enumerate(result["users"], start=1):
            console.print(
                f"{idx}. User {user['user_id']} | "
                f"Suspicious {user['suspicious_transaction_count']} | "
                f"Declined {user['declined_transaction_count']} | "
                f"Avg Score {user['average_risk_score']:.2f} | "
                f"Max Score {user['max_risk_score']:.2f}"
            )
        return
        
    if result_type == "single_category":
        category = result["category"]
        console.print("[bold cyan]Answer[/bold cyan]")
        console.print(f"Group By: {result['group_by']}")
        console.print(f"Value: {category['category_value']}")
        console.print(f"Transaction Count: {category['transaction_count']}")
        return

    if result_type == "category_list":
        console.print("[bold cyan]Answer[/bold cyan]")
        console.print(f"Group By: {result['group_by']}")
        for idx, item in enumerate(result["categories"], start=1):
            console.print(
                f"{idx}. {item['category_value']} | Count {item['transaction_count']}"
            )
        return

    if result_type == "count":
        console.print("[bold cyan]Answer[/bold cyan]")
        console.print(f"Count: {result['count']}")
        console.print(f"Applied Filters: {result['applied_filters']}")
        return

    console.print("[bold cyan]Answer[/bold cyan]")
    console.print(result)


@app.command()
def ask_dataset(
    question: str = typer.Option(..., help="Natural-language question"),
) -> None:
    logger.info("Parsing question with OpenAI tool calling")

    try:
        structured_query = parse_question_with_llm(question)
    except QueryLLMParserError as exc:
        console.print("[bold yellow]Unsupported question[/bold yellow]")
        console.print(str(exc))
        console.print()
        console.print("Supported examples:")
        console.print('- What is the 3rd most fraudulent transaction?')
        console.print('- Which user has the most suspicious transactions?')
        console.print('- Show top 5 highest-risk transactions')
        console.print('- How many declined transactions belong to user 32?')
        return

    logger.info("Running deterministic SQL query")
    logger.info("Structured query: %s", structured_query.model_dump())
    result = run_structured_query(structured_query)

    print_dataset_query_response(
        question=question,
        structured_query=structured_query.model_dump(),
        result=result,
    )


if __name__ == "__main__":
    app()