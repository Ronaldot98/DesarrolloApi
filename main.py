from decimal import Decimal
from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
import os
import urllib

import asyncpg
from sqlalchemy.sql.sqltypes import DECIMAL


DATABASE_URL = "postgres://svtsdvbrzvilfj:770273d026093c94ba6d544a0127f99681f680e783af36d5e621d2846325738b@ec2-44-196-146-152.compute-1.amazonaws.com:5432/dfh76u3rn0t5u2"

#creacion de variables para establecer la conexion a la base de datos 
host_server = os.environ.get('host_server', 'localhost')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '5432')))
database_name = os.environ.get('database_name', 'fastapi')  
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'postgres')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', '1234')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))

#se establece la conexion
#DATABASE_URL = 'postgresql://{}:{}@{}:{}/{}?sslmode={}'.format(db_username, db_password, host_server, db_server_port, database_name, ssl_mode)

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

cliente = sqlalchemy.Table(
    "cliente",
    metadata,
    sqlalchemy.Column("rut", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("nombre", sqlalchemy.String),
    sqlalchemy.Column("direccion", sqlalchemy.String),
    sqlalchemy.Column("telefono", sqlalchemy.String)   
)

proveedor= sqlalchemy.Table(
    "proveedor",
    metadata,
    sqlalchemy.Column("rut", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("nombre", sqlalchemy.String),
    sqlalchemy.Column("direccion", sqlalchemy.String),
    sqlalchemy.Column("telefono", sqlalchemy.String)   
)

producto= sqlalchemy.Table(
    "producto",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("nombre_producto", sqlalchemy.String),
    sqlalchemy.Column("precio", sqlalchemy.DECIMAL),
    sqlalchemy.Column("stock", sqlalchemy.INT)   
)

empleado= sqlalchemy.Table(
    "empleado",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("nombre", sqlalchemy.String),
    sqlalchemy.Column("apellido", sqlalchemy.String),
    sqlalchemy.Column("direccion", sqlalchemy.String),
    sqlalchemy.Column("telefono", sqlalchemy.String),
    sqlalchemy.Column("salario", sqlalchemy.DECIMAL),
    sqlalchemy.Column("precio", sqlalchemy.DECIMAL),
    sqlalchemy.Column("stock", sqlalchemy.INT)   
)


engine = sqlalchemy.create_engine(
    #DATABASE_URL, connect_args={"check_same_thread": False}
    DATABASE_URL, pool_size=3, max_overflow=0
)
metadata.create_all(engine)

class EmpleadoIn(BaseModel):
    nombre: str
    apellido:str
    direccion:str
    telefono:str
    salario:DECIMAL
    precio:DECIMAL
    stock:DECIMAL

class Empleado(BaseModel):
    id: int
    nombre: str
    apellido:str
    direccion:str
    telefono:str
    salario:DECIMAL
    precio:DECIMAL
    stock:DECIMAL

class ClienteIn(BaseModel):
    nombre: str
    direccion:str
    telefono:str    

class Cliente(BaseModel):
    id: int
    nombre: str
    direccion:str
    telefono:str   


class ProveedorIn(BaseModel):
    nombre: str
    direccion:str
    telefono:str    

class Proveedor(BaseModel):
    id: int
    nombre: str
    direccion:str
    telefono:str   



class ProductoIn(BaseModel):
    nombre_producto: str
    precio:DECIMAL
    stock:DECIMAL
class Producto(BaseModel):
    id: int
    nombre_producto: str
    precio:DECIMAL
    stock:DECIMAL

app = FastAPI(title="REST API UMG")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)
app.add_middleware(GZipMiddleware)

@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.post("/empleados/", response_model=Empleado, status_code = status.HTTP_201_CREATED)
async def create_empleado(empleado: EmpleadoIn):
    query = empleado.insert().values(nombre=empleado.nombre,apellido=empleado.apellido,direccion=empleado.direccion,telefono=empleado.telefono,salario=empleado.salario,precio=empleado.precio,stock=empleado.stock)
    last_record_id = await database.execute(query)
    return {**empleado.dict(), "id": last_record_id}

@app.get("/getEmpleado/",response_model=List[Empleado] )
async def getEmpleado(skip: int=0, take: int=20):
    query= empleado.select().offset(skip).limit(take)
    return await database.fetch_all(query)



@app.get("/getEmpleado/{empleado_id}",response_model=Empleado)
async def getEmpleadoId(emp_id: int ):
    query= empleado.select().where(empleado.c.id==emp_id  )
    return await database.fetch_one(query)

@app.delete("/empleadoDelete/{empleado_id}/")
async def del_empleado(emp_id: int):
    query = empleado.delete().where(empleado.c.id==emp_id)
    await database.execute(query)
    return {"message":" Empleado with id:{} deleted succesfully!".format(emp_id)}

@app.put("/empleadoUpdate/{emp_id}",response_model=Empleado)
async def setEmpleadoId(emp_id: int,emp:EmpleadoIn):
    query = empleado.update().where(empleado.c.id==emp_id).values(nombre=emp.nombre, apellido=emp.apellido,status=emp.status)
    await database.execute(query)
    return {**emp.dict(),"id":emp_id}





@app.post("/clientes/", response_model=Cliente, status_code = status.HTTP_201_CREATED)
async def create_cliente(cliente: ClienteIn):
    query = cliente.insert().values(nombre=cliente.nombre,direccion=cliente.direccion,telefono=cliente.telefono)
    last_record_id = await database.execute(query)
    return {**cliente.dict(), "id": last_record_id}



@app.get("/getCliente/",response_model=List[Cliente] )
async def getCliente(skip: int=0, take: int=20):
    query= cliente.select().offset(skip).limit(take)
    return await database.fetch_all(query)



@app.get("/getCliente/{cliente_id}",response_model=Cliente)
async def getClienteId(cliente_id: int ):
    query= cliente.select().where(cliente.c.id==cliente_id )
    return await database.fetch_one(query)

@app.delete("/clienteDelete/{cliente_id}/")
async def del_cliente(cliente_id: int):
    query = cliente.delete().where(cliente.c.id==cliente_id)
    await database.execute(query)
    return {"message":" Cliente with id:{} deleted succesfully!".format(cliente_id)}

@app.put("/clienteUpdate/{cliente_id}",response_model=Cliente)
async def setClienteId(cliente_id: int,cliente:ClienteIn):
    query = empleado.update().where(empleado.c.id==cliente_id).values(nombre=cliente.nombre, direccion=cliente.direccion,telefono=cliente.telefono)
    await database.execute(query)
    return {**cliente.dict(),"id":cliente_id}



#-----
@app.post("/proveedor/",response_model=Proveedor)
async def create_proveedor(proov:ProveedorIn):
    query= proveedor.insert().values(nombre=proov.nombre,direccion=proov.apellido, telefono=proov.telefono)
    
    last_record_id =await database.execute(query)
    return {**proov.dict(), "id":last_record_id}

@app.get("/getProveedor/",response_model=List[Proveedor] )
async def getProveedor(skip: int=0, take: int=20):
    query= proveedor.select().offset(skip).limit(take)
    return await database.fetch_all(query)



@app.get("/getProveedor/{proveedor_id}",response_model=Proveedor)
async def getEmpleadoId(pov_id: int ):
    query= proveedor.select().where(proveedor.c.id==pov_id )
    return await database.fetch_one(query)

@app.delete("/proveedorDelete/{proveedor_id}/")
async def del_proveedor(pov_id: int):
    query = proveedor.delete().where(proveedor.c.id==pov_id)
    await database.execute(query)
    return {"message":" Proveedor with id:{} deleted succesfully!".format(pov_id)}

@app.put("/proveedorUpdate/{prov_id}",response_model=Proveedor)
async def setProveedorId(pov_id: int,pov:ProveedorIn):
    query = proveedor.update().where(proveedor.c.id==pov_id).values(nombre=pov.nombre, direccion=pov.direccion,telefono=pov.telefono)
    await database.execute(query)
    return {**pov.dict(),"id":pov_id}
    

#---

@app.post("/producto/",response_model=Producto)
async def create_producto(prod:ProductoIn):
    query= producto.insert().values(nombre_producto=prod.nombre_producto,precio=prod.precio, stock=prod.stock)
    
    last_record_id =await database.execute(query)
    return {**prod.dict(), "id":last_record_id}

@app.get("/getProducto/",response_model=List[Producto] )
async def getProducto(skip: int=0, take: int=20):
    query= producto.select().offset(skip).limit(take)
    return await database.fetch_all(query)



@app.get("/getProducto/{producto_id}",response_model=Producto)
async def getProductoId(prod_id: int ):
    query= producto.select().where(producto.c.id==prod_id )
    return await database.fetch_one(query)

@app.delete("/productoDelete/{producto_id}/")
async def del_producto(prod_id: int):
    query = producto.delete().where(producto.c.id==prod_id)
    await database.execute(query)
    return {"message":" Producto with id:{} deleted succesfully!".format(prod_id)}

@app.put("/productoUpdate/{prov_id}",response_model=Producto)
async def setProductoId(prod_id: int,prod:ProductoIn):
    query = producto.update().where(producto.c.id==prod_id).values(nombre_producto=prod.nombre_producto,precio=prod.precio, stock=prod.stock)
    await database.execute(query)
    return {**prod.dict(),"id":prod_id}
    
