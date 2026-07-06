import os
import great_expectations as gx
from dotenv import load_dotenv

load_dotenv()


def build_connection_string(db_config=None):
    if db_config is None:
        db_config = {
            'host': 'localhost',
            'port': os.getenv('POSTGRES_PORT'),
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
    return (f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")


def run_trade_checkpoint(db_config=None):
    context = gx.get_context(mode="ephemeral")

    ds = context.data_sources.add_postgres(
        name="pipeline_pg",
        connection_string=build_connection_string(db_config)
    )
    asset = ds.add_table_asset(name="trade_raw", table_name="trade_raw",
                               schema_name="bronze")
    batch_def = asset.add_batch_definition_whole_table("full_table")

    suite = gx.ExpectationSuite(name="bronze_trade_suite")

    for col in ["cty_code", "cty_name", "hs_code", "trade_value_usd", "month"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="trade_value_usd", regex=r"^[0-9]+$")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="month", regex=r"^[0-9]{4}-[0-9]{2}$")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="hs_code",
            value_set=["854231", "854232", "854233", "854239", "854290"])
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="cty_code", regex=r"^(-|[0-9]{4}|[0-9]XXX|00[0-9]{2})$")
    )

    context.suites.add(suite)

    validation_def = gx.ValidationDefinition(
        name="trade_validation", data=batch_def, suite=suite
    )
    context.validation_definitions.add(validation_def)

    result = validation_def.run()

    stats = {
        "success": result.success,
        "checked": len(result.results),
        "failed": sum(1 for r in result.results if not r.success)
    }
    print(f"GE checkpoint: success={stats['success']}, "
          f"expectations={stats['checked']}, failed={stats['failed']}")
    if not result.success:
        for r in result.results:
            if not r.success:
                print(f"  FAILED: {r.expectation_config.type} "
                      f"on {r.expectation_config.kwargs.get('column')}")
    return stats


if __name__ == "__main__":
    stats = run_trade_checkpoint()
    if not stats["success"]:
        raise SystemExit(1)