import pandas as pd

logger = __import__("kedro").get_logger(__name__)


def create_flight_company_dimension(
    airline_id: pd.DataFrame,
    carrier_history: pd.DataFrame,
    carrier_group_new: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a flight company dimension table.

    Args:
        airline_id: Preprocessed data for airline IDs.
        carrier_history: Preprocessed data for carrier history.
        carrier_group_new: Preprocessed data for carrier group new.
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
    airport_id: pd.DataFrame,
    city_market_id: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create an airport dimension table.

    Args:
        airport_id: Preprocessed data for airport IDs.
        city_market_id: Preprocessed data for city market IDs.
    Returns:
        Airport dimension table.

    """
    logger.info("Creating airport dimension table")

    airport_dimension = airport_id.rename(
        columns={"Code": "id_airport", "Description": "ds_aeroporto_cidade_pais"},
        inplace=True,
    )

    airport_dimension["nm_cidade"] = airport_dimension[
        "ds_aeroporto_cidade_pais"
    ].apply(lambda x: x.split(":")[0])

    airport_dimension = airport_dimension.insert(
        0, "id_aeroporto_sk", range(1, len(airport_dimension) + 1)
    )

    return airport_dimension


def create_distance_dimension(
    distance_group_500: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a distance dimension table.

    Args:
        distance_group_500: Preprocessed data for distance groups.
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
    m2 = market_data_2024["CARRIER_NAME"].str.contains("Cape Air")

    operator_dimension["CARRIER"] = market_data_2024.loc[~m1, "CARRIER"].unique()
    operator_dimension["CARRIER_NAME"] = market_data_2024.loc[
        ~m2, "CARRIER_NAME"
    ].unique()
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
    airport_id: pd.DataFrame,
    carrier_history: pd.DataFrame,
    operator_dimension: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a company dimension table.

    Args:
        carrier_group_new: Preprocessed data for carrier group new.
        airport_id: Preprocessed data for airport IDs.
        carrier_history: Preprocessed data for carrier history.
    Returns:
        Company dimension table.

    """
    logger.info("Creating company dimension table")

    carrier_dimension = pd.merge(
        airport_id,
        carrier_history,
        left_on="Code",
        right_on="AirlineID",
        how="left",
    )

    carrier_dimension = pd.merge(
        carrier_dimension,
        operator_dimension,
        left_on="CarrierGroup",
        right_on="id_carrier_group",
        how="left",
    )

    carrier_dimension = (
        carrier_dimension[["Code", "Description", "id_operadora_sk"]]
        .rename(
            columns={"Code": "id_airline", "Description": "nm_companhia"}, inplace=True
        )
        .insert(0, "id_companhia_sk", range(1, len(carrier_dimension) + 1))
    )

    carrier_dimension["id_operadora_sk"] = (
        carrier_dimension["id_operadora_sk"].fillna(0).astype(int)
    )  # Usando 0 para "Não especificado"

    return carrier_dimension
