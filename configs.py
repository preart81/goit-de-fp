#
# ? Конфігурація Kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# ? Визначення топіків
my_name = "Artur"

topic_names_dict = {
    "athlete_event_results": f"{my_name}_athlete_event_results",
    "enriched_athlete_avg": f"{my_name}_enriched_athlete_avg",
}
