from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_airport_dimension,
    create_company_dimension,
    create_distance_dimension,
    create_flight_company_dimension,
    create_operator_dimension,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_flight_company_dimension,
                inputs=["airline", "carrier_history", "carrier_group_new"],
                outputs="flight_company_dimension",
                name="create_flight_company_dimension_node",
            ),
            node(
                func=create_airport_dimension,
                inputs=["airport_id", "city_market_id"],
                outputs="airport_dimension",
                name="create_airport_dimension_node",
            ),
            node(
                func=create_distance_dimension,
                inputs=["distance_group_500"],
                outputs="distance_dimension",
                name="create_distance_dimension_node",
            ),
            node(
                func=create_operator_dimension,
                inputs=["market_data_2024"],
                outputs="operator_dimension",
                name="create_operator_dimension_node",
            ),
            node(
                func=create_company_dimension,
                inputs=["airport_id", "carrier_history", "operator_dimension"],
                outputs="company_dimension",
                name="create_company_dimension_node",
            ),
        ]
    )
