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
        columns={"Code": "id_distance_group", "Description": "ds_faixa_distancia"},
        inplace=True,
    )

    distance_dimension = distance_dimension.insert(
        0, "id_distancia_sk", range(1, len(distance_dimension) + 1)
    )

    return distance_dimension


def create_operator_dimension(
    carrier_group_new: pd.DataFrame,
    airport_id: pd.DataFrame,
    carrier_history: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a operator dimension table.

    Args:
        carrier_group_new: Preprocessed data for carrier group new.
        airport_id: Preprocessed data for airport IDs.
        carrier_history: Preprocessed data for carrier history.
    Returns:
        Operator dimension table.

    """
    logger.info("Creating operator dimension table")

    operator_dimension = carrier_group_new.rename(
        columns={"Code": "id_carrier_group", "Description": "nm_carrier_group"},
        inplace=True,
    )

    operator_dimension = operator_dimension.insert(
        0, "id_operadora_sk", range(1, len(operator_dimension) + 1)
    )

    return operator_dimension


def create_carrier_dimension(
    airport_id: pd.DataFrame,
    carrier_history: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a carrier dimension table.

    Args:
        carrier_group_new: Preprocessed data for carrier group new.
        airport_id: Preprocessed data for airport IDs.
        carrier_history: Preprocessed data for carrier history.
    Returns:
        Carrier dimension table.

    """
    logger.info("Creating carrier dimension table")

    carrier_dimension = airport_id.rename(
        columns={"Code": "id_airline", "Description": "nm_companhia"}, inplace=True
    )

    carrier_dimension = carrier_dimension.insert(
        0, "id_companhia_sk", range(1, len(carrier_dimension) + 1)
    )

    return carrier_dimension
