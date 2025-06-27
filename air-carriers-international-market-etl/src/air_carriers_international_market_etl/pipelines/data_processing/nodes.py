import pandas as pd

logger = __import__("kedro").get_logger(__name__)


def create_flight_company_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a flight company dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Flight company dimension table.

    """
    logger.info("Creating flight company dimension table")

    rated_shuttles = airline_id.merge(
        carrier_history, left_on="id", right_on="shuttle_id"
    )
    rated_shuttles = rated_shuttles.drop("id", axis=1)
    model_input_table = rated_shuttles.merge(
        carrier_group_new, left_on="company_id", right_on="id"
    )
    model_input_table = model_input_table.dropna()
    return model_input_table


def create_airport_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create an airport dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Airport dimension table.

    """
    logger.info("Creating airport dimension table")

    airport_dimension = pd.DataFrame()

    origin_cols = {
        "ORIGIN_AIRPORT_ID": "AIRPORT_ID",
        "ORIGIN_AIRPORT_SEQ_ID": "AIRPORT_SEQ_ID",
        "ORIGIN_CITY_MARKET_ID": "CITY_MARKET_ID",
        "ORIGIN_CITY_NAME": "CITY_NAME",
        "ORIGIN_COUNTRY": "COUNTRY",
        "ORIGIN_COUNTRY_NAME": "COUNTRY_NAME",
        "ORIGIN_WAC": "WAC",
    }
    dest_cols = {
        "DEST_AIRPORT_ID": "AIRPORT_ID",
        "DEST_AIRPORT_SEQ_ID": "AIRPORT_SEQ_ID",
        "DEST_CITY_MARKET_ID": "CITY_MARKET_ID",
        "DEST_CITY_NAME": "CITY_NAME",
        "DEST_COUNTRY": "COUNTRY",
        "DEST_COUNTRY_NAME": "COUNTRY_NAME",
        "DEST_WAC": "WAC",
    }

    origin_df = market_data_2024[list(origin_cols.keys())].rename(columns=origin_cols)
    dest_df = market_data_2024[list(dest_cols.keys())].rename(columns=dest_cols)

    airport_dimension = (
        pd.concat([origin_df, dest_df], ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    airport_dimension["id_aeroporto"] = airport_dimension.reset_index().index + 1

    return airport_dimension


def create_distance_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a distance dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024..
    Returns:
        Distance dimension table.

    """
    logger.info("Creating distance dimension table")

    distance_dimension = distance_group_500.rename(
        columns={"id_distance_group": "Code", "ds_faixa_distancia": "Description"},
    )

    distance_dimension = distance_dimension.insert(
        0, "id_distancia_sk", range(1, len(distance_dimension) + 1)
    )

    return distance_dimension


def create_time_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a time dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024..
    Returns:
        Time dimension table.

    """
    logger.info("Creating time dimension table")

    time_dimension = pd.DataFrame()

    time_dimension = market_data_2024[["YEAR", "QUARTER", "MONTH"]].drop_duplicates()
    time_dimension["id_tempo"] = time_dimension.reset_index().index + 1

    return time_dimension


def create_operator_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a operator dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Operator dimension table.

    """
    logger.info("Creating operator dimension table")

    operator_dimension = pd.DataFrame()

    # Removing 9k from the CARRIER column
    m1 = market_data_2024["CARRIER"].str.contains("9K")

    operator_dimension = market_data_2024.loc[
        ~m1, ["CARRIER", "CARRIER_NAME"]
    ].drop_duplicates()
    operator_dimension = (
        pd.merge(
            operator_dimension,
            market_data_2024[["CARRIER", "CARRIER_GROUP", "CARRIER_GROUP_NEW"]],
            on="CARRIER",
            how="left",
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )
    operator_dimension["id_operadora"] = operator_dimension.index + 1

    logger.info("Operator dimension table created successfully")

    return operator_dimension


def create_company_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a company dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Company dimension table.

    """
    logger.info("Creating company dimension table")

    company_dimension = pd.DataFrame()

    # Removing 9k from the CARRIER column
    m1 = market_data_2024["CARRIER"].str.contains("9K")

    company_dimension = market_data_2024.loc[
        ~m1,
        [
            "AIRLINE_ID",
            "UNIQUE_CARRIER",
            "UNIQUE_CARRIER_NAME",
            "UNIQUE_CARRIER_ENTITY",
            "CARRIER",
            "CARRIER_NAME",
            "REGION",
        ],
    ].drop_duplicates()
    company_dimension["id_companhia"] = company_dimension.reset_index().index + 1

    company_dimension.columns

    return company_dimension
