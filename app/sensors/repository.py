from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.mongodb_client import MongoDBClient
import json
import copy

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, mongodb_client: MongoDBClient, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude, type=sensor.type,
    mac_address=sensor.mac_address, manufacturer=sensor.manufacturer, model=sensor.model, serie_number=sensor.serie_number,
    firmware_version=sensor.firmware_version)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mongodb_document = {"id": db_sensor.id, "name": sensor.name, "latitude": sensor.latitude, "longitude": sensor.longitude,
    "type": sensor.type, "mac_address": sensor.mac_address, "manufacturer": sensor.manufacturer, "model": sensor.model,
    "serie_number": sensor.serie_number, "firmware_version": sensor.firmware_version}
    mongodb_database = mongodb_client.getDatabase("sensors")

    mongodb_collection = None
    if sensor.type == "Temperatura":
        mongodb_collection = mongodb_client.getCollection("sensors temperatura")
    elif sensor.type == "Velocitat":
        mongodb_collection = mongodb_client.getCollection("sensors velocitat")
    mongodb_collection.insert_one(mongodb_document)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = data

    if db_sensordata.temperature is not None:
        redis.set(f'sensor {sensor_id} temperature', db_sensordata.temperature)
        redis.set(f'sensor {sensor_id} name', f'Sensor Temperatura {sensor_id}')
    if db_sensordata.velocity is not None:
        redis.set(f'sensor {sensor_id} velocity', db_sensordata.velocity)
        redis.set(f'sensor {sensor_id} name', f'Sensor Velocitat {sensor_id}')
    if db_sensordata.humidity is not None:
        redis.set(f'sensor {sensor_id} humidity', db_sensordata.humidity)


        
    # Fem una relacio clau-valor amb la clau sent "sensor sensor_id" i el nom de la propietat
    redis.set(f'sensor {sensor_id} name', db_sensordata.name)
    redis.set(f'sensor {sensor_id} id', sensor_id)
    redis.set(f'sensor {sensor_id} battery_level', db_sensordata.battery_level)
    redis.set(f'sensor {sensor_id} last_seen', db_sensordata.last_seen)
    return db_sensordata

def get_data(redis: Session, sensor_id: int) -> schemas.Sensor:
    # Primer mirem si existeix un sensor amb aquest id
    id_sensor = redis.get(f'sensor {sensor_id} id')
    # Si no existeix retornem None
    if id_sensor is None:
        return None

    # Finalmente convertim els valors obtinguts en el seu tipus corresponent
    id_sensor = int(id_sensor)
    name = redis.get(f'sensor {sensor_id} name')    
    battery_level = float(redis.get(f'sensor {sensor_id} battery_level'))
    last_seen = redis.get(f'sensor {sensor_id} last_seen')
    sensor_json = {"id": id_sensor, "name": name, "battery_level": battery_level, "last_seen": last_seen}

    humidity = redis.get(f'sensor {sensor_id} humidity')
    temperature = redis.get(f'sensor {sensor_id} temperature')
    velocity = redis.get(f'sensor {sensor_id} velocity')

    if humidity is not None:
        sensor_json["humidity"] = float(humidity)
    if temperature is not None:
        sensor_json["temperature"] = float(temperature)
    if velocity is not None:
        sensor_json["velocity"] = float(velocity)
    return sensor_json 

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(mongodb: MongoDBClient,  redis_client: Session, latitude: float, longitude: float):
    query = {"latitude": latitude, "longitude": longitude}
    mongodb_database = mongodb.getDatabase("sensors")
    mongodb_collection_velocitat = mongodb.getCollection("sensors velocitat")
    sensors_velocitat = mongodb_collection_velocitat.find(query)
    mongodb_collection_temperatura = mongodb.getCollection("sensors temperatura")
    sensors_temperatura = mongodb_collection_temperatura.find(query)

    sensors = []
    for sensor_temperatura in sensors_temperatura:
        sensors.append(get_data(redis_client, sensor_temperatura['id']))
    
    for sensor_velocitat in sensors_velocitat:
        sensors.append(get_data(redis_client, sensor_velocitat['id']))
    

    return sensors
