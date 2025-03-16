# Databricks notebook source
# MAGIC %md 
# MAGIC #Introducción
# MAGIC
# MAGIC En este notebook vamos a simular la producción de eventos o mensajes en kafka
# MAGIC
# MAGIC Para poder seguir con este ejercicio, necesitamos tener creada y configurada la cuenta en **Confluent Cloud**

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos la configuración del cliente kafka desde DBFS

# COMMAND ----------

def read_config():
  config = {}
  with open("/dbfs/FileStore/client_properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

conf = read_config()

# COMMAND ----------

# MAGIC %md
# MAGIC Listamos los topics actuales

# COMMAND ----------

from confluent_kafka.admin import AdminClient 

admin_client = AdminClient(conf) 
topic_list = admin_client.list_topics().topics 
print("Topics en el cluster de Kafka:") 
topic_list 

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos un nuevo topic

# COMMAND ----------

from confluent_kafka.admin import NewTopic 

# Creación de un topic 
topic = "orders"
new_topic = NewTopic(topic, num_partitions=3, replication_factor=3)
fs = admin_client.create_topics([new_topic])

# Esperamos a que termine la operación 
for topic, f in fs.items(): 
    try: 
        f.result() # The result itself is None 
        print("Topic {} creado".format(topic)) 
    except Exception as e: 
        print("Error al crear el topic {}: {}".format(topic, e)) 

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora deberíamos ver el nuevo topic creado

# COMMAND ----------

topic_list = admin_client.list_topics().topics 

print(f"Topics en el cluster de Kafka:\n{topic_list}") 

# COMMAND ----------

# MAGIC %md
# MAGIC Para crear datos aleatorios haremos uso de una librería python llamada faker.
# MAGIC
# MAGIC Como solo la vamos a usar para producir (en el driver), no hace falta que la instalemos en el cluster

# COMMAND ----------

# MAGIC %md
# MAGIC Voy a reutilizar un desarrollo para crear eventos de pedidos de pizzas.
# MAGIC
# MAGIC El código de la siguiente celda lo he adaptado del siguiente repositorio en github:
# MAGIC
# MAGIC https://github.com/Aiven-Labs/python-fake-data-producer-for-apache-kafka

# COMMAND ----------

import random
import time
import json
from faker.providers import BaseProvider

class PizzaProvider(BaseProvider):
    def pizza_name(self):
        valid_pizza_names = [
            "Margherita",
            "Marinara",
            "Diavola",
            "Mari & Monti",
            "Salami",
            "Peperoni",
        ]
        return random.choice(valid_pizza_names)

    def pizza_topping(self):
        available_pizza_toppings = [
            "🍅 tomato",
            "🧀 blue cheese",
            "🥚 egg",
            "🫑 green peppers",
            "🌶️ hot pepper",
            "🥓 bacon",
            "🫒 olives",
            "🧄 garlic",
            "🐟 tuna",
            "🧅 onion",
            "🍍 pineapple",
            "🍓 strawberry",
            "🍌 banana",
        ]
        return random.choice(available_pizza_toppings)

    def pizza_shop(self):
        pizza_shops = [
            "Marios Pizza",
            "Luigis Pizza",
            "Circular Pi Pizzeria",
            "Ill Make You a Pizza You Can" "t Refuse",
            "Mammamia Pizza",
            "Its-a me! Mario Pizza!",
        ]
        return random.choice(pizza_shops)

    def produce_msg(
        self,
        FakerInstance,
        ordercount=1,
        max_pizzas_in_order=5,
        max_toppings_in_pizza=3,
    ):
        shop = FakerInstance.pizza_shop()
        # Each Order can have 1-10 pizzas in it
        pizzas = []
        for pizza in range(random.randint(1, max_pizzas_in_order)):
            # Each Pizza can have 0-5 additional toppings on it
            toppings = []
            for topping in range(random.randint(1, max_toppings_in_pizza)):
                toppings.append(FakerInstance.pizza_topping())
            pizzas.append(
                {
                    "pizzaName": FakerInstance.pizza_name(),
                    "additionalToppings": toppings,
                }
            )
        # message composition
        value = {
            "id": ordercount,
            "shop": shop,
            "name": FakerInstance.unique.name(),
            "phoneNumber": FakerInstance.unique.phone_number(),
            "address": FakerInstance.address(),
            "pizzas": pizzas,
            "timestamp": int(time.time() * 1000),
        }
        key = shop
        return key, value

# COMMAND ----------

# MAGIC %md
# MAGIC Producimos 10 mensajes

# COMMAND ----------

from confluent_kafka import Producer 
from faker import Faker

producer = Producer(conf) 
fake = Faker()
fake.add_provider(PizzaProvider)
MAX_NUMBER_PIZZAS_IN_ORDER = 3
MAX_ADDITIONAL_TOPPINGS_IN_PIZZA = 5
MAX_ORDERS = 10

for i in range(MAX_ORDERS):
    key,value = fake.produce_msg(
                    fake,
                    i+1,
                    MAX_NUMBER_PIZZAS_IN_ORDER,
                    MAX_ADDITIONAL_TOPPINGS_IN_PIZZA,
                )
    producer.produce(topic=topic, key=key, value=json.dumps(value))
    producer.flush()
    print(f"pizza {i+1} ordered")
    # sleeping time
    sleep_time = (
        random.randint(0, int(1 * 10000)) / 10000
    )
    print("sleeping for..." + str(sleep_time) + "s")
    time.sleep(sleep_time)