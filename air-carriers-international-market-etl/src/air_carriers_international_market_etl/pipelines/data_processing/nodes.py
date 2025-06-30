import pandas as pd


def create_class_dimension(
    market_data_2024: pd.DataFrame,
    service_class: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a flight company dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Flight company dimension table.

    """
    class_dimension = pd.DataFrame()

    class_dimension = market_data_2024[["CLASS"]].drop_duplicates()
    class_dimension = class_dimension.merge(
        service_class,
        left_on="CLASS",
        right_on="Code",
        how="left",
    ).drop(columns=["CLASS"])

    class_dimension["id_class"] = class_dimension.reset_index().index + 1

    return class_dimension


def create_airport_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create an airport dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Airport dimension table.

    """
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
    distance_dimension = pd.DataFrame()

    # not implemented Faixa_Descricao
    distance_dimension = market_data_2024[["DISTANCE_GROUP"]].drop_duplicates()
    distance_dimension["id_distancia"] = distance_dimension.reset_index().index + 1

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


def create_fact_table(
    market_data_2024: pd.DataFrame,
    dim_tempo: pd.DataFrame,
    dim_companhia: pd.DataFrame,
    dim_aeroporto: pd.DataFrame,
    dim_distancia: pd.DataFrame,
    dim_classe: pd.DataFrame,
    dim_operadora: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria a tabela de fatos 'Fato_Voos' a partir dos dados de mercado e das dimensões pré-construídas.

    A função realiza a junção (merge) dos dados transacionais com cada uma das
    dimensões para substituir as chaves de negócio pelas chaves substitutas (surrogate keys)
    do Data Warehouse.

    Args:
        market_data_2024 (pd.DataFrame): DataFrame contendo os dados brutos de voos.
        dim_tempo (pd.DataFrame): Tabela de dimensão de tempo.
        dim_companhia (pd.DataFrame): Tabela de dimensão de companhia aérea.
        dim_aeroporto (pd.DataFrame): Tabela de dimensão de aeroportos.
        dim_distancia (pd.DataFrame): Tabela de dimensão de distância.
        dim_classe (pd.DataFrame): Tabela de dimensão de classe de serviço.

    Returns:
        pd.DataFrame: A tabela de fatos 'Fato_Voos' finalizada.
    """
    fact_table = market_data_2024.copy()

    # Merge com Dim_Tempo
    fact_table = pd.merge(
        fact_table, dim_tempo, on=["YEAR", "QUARTER", "MONTH"], how="left"
    )

    # Merge com Dim_Companhia
    company_cols = [
        "AIRLINE_ID",
        "UNIQUE_CARRIER",
        "UNIQUE_CARRIER_NAME",
        "UNIQUE_CARRIER_ENTITY",
        "CARRIER",
        "CARRIER_NAME",
        "REGION",
    ]
    fact_table = pd.merge(fact_table, dim_companhia, on=company_cols, how="left")

    # Merge com Dim_Aeroporto para Origem
    origin_cols_map = {
        "ORIGIN_AIRPORT_ID": "AIRPORT_ID",
        "ORIGIN_AIRPORT_SEQ_ID": "AIRPORT_SEQ_ID",
        "ORIGIN_CITY_MARKET_ID": "CITY_MARKET_ID",
        "ORIGIN_CITY_NAME": "CITY_NAME",
        "ORIGIN_COUNTRY": "COUNTRY",
        "ORIGIN_COUNTRY_NAME": "COUNTRY_NAME",
        "ORIGIN_WAC": "WAC",
    }
    fact_table_origin_merged = pd.merge(
        fact_table,
        dim_aeroporto,
        left_on=list(origin_cols_map.keys()),
        right_on=list(origin_cols_map.values()),
        how="left",
    ).rename(columns={"id_aeroporto": "id_origem"})

    # Merge com Dim_Aeroporto para Destino
    dest_cols_map = {
        "DEST_AIRPORT_ID": "AIRPORT_ID",
        "DEST_AIRPORT_SEQ_ID": "AIRPORT_SEQ_ID",
        "DEST_CITY_MARKET_ID": "CITY_MARKET_ID",
        "DEST_CITY_NAME": "CITY_NAME",
        "DEST_COUNTRY": "COUNTRY",
        "DEST_COUNTRY_NAME": "COUNTRY_NAME",
        "DEST_WAC": "WAC",
    }
    # Usamos o resultado do merge anterior para adicionar o id_destino
    fact_table_merged = pd.merge(
        fact_table_origin_merged,
        dim_aeroporto,
        left_on=list(dest_cols_map.keys()),
        right_on=list(dest_cols_map.values()),
        how="left",
        suffixes=("", "_dest"),
    ).rename(columns={"id_aeroporto": "id_destino"})

    # Merge com Dim_Distancia
    fact_table_merged = pd.merge(
        fact_table_merged, dim_distancia, on="DISTANCE_GROUP", how="left"
    )

    # Merge com Dim_Classe
    fact_table_merged = pd.merge(fact_table_merged, dim_classe, on="CLASS", how="left")

    # Selecionando as colunas de fatos e as chaves estrangeiras
    final_fact_table = fact_table_merged[
        [
            "id_tempo",
            "id_companhia",
            "id_origem",
            "id_destino",
            "id_distancia",
            "id_classe",
            "PASSENGERS",
            "FREIGHT",
            "MAIL",
            "DISTANCE",
        ]
    ]

    return final_fact_table
