import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

# Crear una instancia de Faker para generar datos aleatorios
fake = Faker()

# Configuración de la conexión a la base de datos
conn = psycopg2.connect(
    dbname="Parada_1",
    user="postgres",
    password="1234",
    host="localhost"
)

# Creación de un cursor         
cur = conn.cursor()

# Función para poblar la tabla `Ubicacion` con clave primaria manual
def populate_ubicacion(start_id, n):
    for i in range(n):
        id_ubicacion = start_id + i
        codigo_postal = fake.postcode()
        barrio = fake.city_suffix()
        localidad = fake.city()
        provincia = fake.state()
        
        cur.execute("""
            INSERT INTO transaccional.Ubicacion (ID_Ubicacion, Codigo_postal, Barrio, Localidad, Provincia)
            VALUES (%s, %s, %s, %s, %s)
        """, (id_ubicacion, codigo_postal, barrio, localidad, provincia))
    conn.commit()
    print(f"{n} registros insertados en `Ubicacion`")

# Función para poblar la tabla `Parada` con clave primaria manual
def populate_parada(start_id, n):
    for i in range(n):
        id_parada = start_id + i
        id_ubicacion = random.randint(1, 30)  # Asegúrate de tener ubicaciones con estos IDs
        direccion = fake.street_address()
        nombre = fake.word().capitalize()
        
        cur.execute("""
            INSERT INTO transaccional.Parada (ID_Parada, ID_Ubicacion, Direccion, Nombre)
            VALUES (%s, %s, %s, %s)
        """, (id_parada, id_ubicacion, direccion, nombre))
    conn.commit()
    print(f"{n} registros insertados en `Parada`")

# Función para poblar la tabla `Colectivo` con clave primaria manual
def populate_colectivo(start_id, n):
    for i in range(n):
        id_colectivo = start_id + i
        matricula = fake.license_plate()
        antiguedad = random.randint(1, 20)
        marca = fake.company()
        
        cur.execute("""
            INSERT INTO transaccional.Colectivo (ID_Colectivo, Matricula, Antiguedad, Marca)
            VALUES (%s, %s, %s, %s)
        """, (id_colectivo, matricula, antiguedad, marca))
    conn.commit()
    print(f"{n} registros insertados en `Colectivo`")

# Función para poblar la tabla `Clientes` con clave primaria manual
def populate_clientes(start_id, n):
    for i in range(n):
        id_cliente = start_id + i
        id_parada = random.randint(1, 30)  # Asegúrate de tener paradas con estos IDs
        genero = random.choice(['M', 'F'])
        tipo_cliente = random.choice(['Estudiante', 'Trabajador', 'Turista', 'Otro'])
        start_date = datetime.strptime("2023-11-01", "%Y-%m-%d")
        end_date = datetime.strptime("2023-11-30", "%Y-%m-%d")
        fecha_llegada = fake.date_time_between(start_date=start_date, end_date=end_date)
        fecha_salida = fecha_llegada + timedelta(minutes=random.randint(1, 60))
        while True:
            id_colectivo = random.randint(1, 15)  # Asegúrate de tener colectivos con estos IDs
            cur.execute("""
            SELECT 1 
            FROM transaccional.Colectivo_Parada cp
            JOIN transaccional.Parada p ON cp.ID_Parada = p.ID_Parada
            WHERE cp.ID_Colectivo = %s AND p.ID_Parada = %s
            """, (id_colectivo, id_parada))
            
            if cur.fetchone():
                break  # Salir del bucle si se encuentra una coincidencia
        
        cur.execute("""
            INSERT INTO transaccional.Clientes (ID_Cliente, ID_Parada, Genero, Tipo_de_Cliente, Fecha_llegada, Fecha_salida, colectivo_id)
            VALUES (%s, %s, %s, %s, %s, %s,%s)
        """, (id_cliente, id_parada, genero, tipo_cliente, fecha_llegada, fecha_salida,id_colectivo))
    conn.commit()
    print(f"{n} registros insertados en `Clientes`")

# Función para poblar la tabla `Horarios` con clave primaria manual
def populate_horarios(start_id, n):
    for i in range(n):
        id_horario = start_id + i
        hora_salida = fake.time()
        hora_llegada = fake.time()
        dia_semana = random.choice(['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])
        
        cur.execute("""
            INSERT INTO transaccional.Horarios (ID_Horario, hora_salida, hora_llegada, dia_semana)
            VALUES (%s, %s, %s, %s)
        """, (id_horario, hora_salida, hora_llegada, dia_semana))
    conn.commit()
    print(f"{n} registros insertados en `Horarios`")

# Función para poblar la tabla `Ruta` con clave primaria manual
def populate_rutas(start_id, cantidad):
    for i in range(cantidad):
        id_ruta = start_id + i
        nombre_ruta = fake.city() + " - " + fake.city()  # Combina dos ciudades para generar una ruta
        origen = fake.address()
        destino = fake.address()
        distancia_km = round(random.uniform(5, 100), 2)  # Distancia en km entre 5 y 100
        tiempo_estimado = round(distancia_km / random.uniform(30, 60), 2)  # Tiempo en horas

        cur.execute("""
            INSERT INTO transaccional.Ruta (ID_Ruta, nombre_ruta, origen, destino, distancia_km, tiempo_estimado)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (id_ruta, nombre_ruta, origen, destino, distancia_km, tiempo_estimado))
    
    conn.commit()
    print(f"Insertados {cantidad} registros en la tabla Rutas.")

# Función para poblar la tabla `Chofer` con clave primaria manual
def populate_choferes(start_id, cantidad):
    for i in range(cantidad):
        id_chofer = start_id + i
        nombre = fake.first_name()
        apellido = fake.last_name()
        licencia = fake.bothify(text="???-########")
        fecha_expiracion_licencia = fake.date_between(start_date="today", end_date="+3y")
        telefono = fake.phone_number()
        estado = random.choice(['Activo', 'Inactivo'])

        cur.execute("""
            INSERT INTO transaccional.Chofer (ID_Chofer, Nombre, Apellido, licencia, fecha_expiracion_licencia, Telefono, Estado)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (id_chofer, nombre, apellido, licencia, fecha_expiracion_licencia, telefono, estado))
    
    conn.commit()
    print(f"Insertados {cantidad} registros en la tabla Chofer.")

#------------------------------------TABLAS INTERMEDIAS Y RELACIONES------------------------------------
# Función para obtener el rango de IDs en una tabla
def get_id_range(table_name, id_column):
    cur.execute(f"SELECT MIN({id_column}), MAX({id_column}) FROM transaccional.{table_name}")
    start_id, end_id = cur.fetchone()
    return start_id, end_id

# Función para poblar la tabla `Colectivo_Parada` (relación M:N entre Colectivo y Parada)
# Función para poblar la tabla `Colectivo_Parada` (relación M:N entre Colectivo y Parada)
def populate_colectivo_parada(n):
    start_id_colectivo, end_id_colectivo = get_id_range("Colectivo", "ID_Colectivo")
    start_id_parada, end_id_parada = get_id_range("Parada", "ID_Parada")

    for _ in range(n):
        while True:
            id_colectivo = random.randint(start_id_colectivo, end_id_colectivo)
            id_parada = random.randint(start_id_parada, end_id_parada)
            
            # Verificar si la combinación ya existe
            cur.execute("""
                SELECT 1 FROM transaccional.Colectivo_Parada 
                WHERE ID_Colectivo = %s AND ID_Parada = %s
            """, (id_colectivo, id_parada))
            
            if not cur.fetchone():
                # Si no existe, realizar la inserción y salir del bucle
                cur.execute("""
                    INSERT INTO transaccional.Colectivo_Parada (ID_Colectivo, ID_Parada)
                    VALUES (%s, %s)
                """, (id_colectivo, id_parada))
                break  # Salir del bucle while una vez insertado

    conn.commit()
    print(f"{n} registros insertados en `Colectivo_Parada`")

# Función para poblar la tabla `Asignacion_Vehiculo`
def populate_asignacion_vehiculo(n):
    start_id_horario, end_id_horario = get_id_range("Horarios", "ID_Horario")
    start_id_vehiculo, end_id_vehiculo = get_id_range("Colectivo", "ID_Colectivo")

    for _ in range(n):
        id_horario = random.randint(start_id_horario, end_id_horario)
        id_vehiculo = random.randint(start_id_vehiculo, end_id_vehiculo)
        fecha_asignacion = fake.date_between(start_date="-1y", end_date="today")
        
        cur.execute("""
            INSERT INTO transaccional.Asignacion_Vehiculo (id_horario, id_vehiculo, fecha_asignacion)
            VALUES (%s, %s, %s)
        """, (id_horario, id_vehiculo, fecha_asignacion))
    conn.commit()
    print(f"{n} registros insertados en `Asignacion_Vehiculo`")

# Función para poblar la tabla `Asignacion_Conductor`
def populate_asignacion_conductor(n):
    start_id_conductor, end_id_conductor = get_id_range("Chofer", "ID_Chofer")
    start_id_horario, end_id_horario = get_id_range("Horarios", "ID_Horario")

    for _ in range(n):
        id_conductor = random.randint(start_id_conductor, end_id_conductor)
        id_horario = random.randint(start_id_horario, end_id_horario)
        fecha_asignacion = fake.date_between(start_date="-1y", end_date="today")
        
        cur.execute("""
            INSERT INTO transaccional.Asignacion_Conductor (id_conductor, id_horario, fecha_asignacion)
            VALUES (%s, %s, %s)
        """, (id_conductor, id_horario, fecha_asignacion))
    conn.commit()
    print(f"{n} registros insertados en `Asignacion_Conductor`")

# Función para poblar la tabla `horario_y_rutas`
def populate_horario_y_rutas(n):
    start_id_ruta, end_id_ruta = get_id_range("Ruta", "ID_Ruta")
    start_id_horario, end_id_horario = get_id_range("Horarios", "ID_Horario")

    for _ in range(n):
        id_ruta = random.randint(start_id_ruta, end_id_ruta)
        id_horario = random.randint(start_id_horario, end_id_horario)
        
        cur.execute("""
            INSERT INTO transaccional.horario_y_rutas (ID_Ruta, ID_Horario)
            VALUES (%s, %s)
        """, (id_ruta, id_horario))
    conn.commit()
    print(f"{n} registros insertados en `horario_y_rutas`")

# Función para poblar la tabla `linea`
def populate_linea(n):
    for _ in range(n):
        nombre = str(100 + _)  # Genera nombres únicos de línea del 100 al 130

        cur.execute("""
            INSERT INTO transaccional.linea (nombre)
            VALUES (%s)
        """, (nombre,))
    conn.commit()
    print(f"{n} registros insertados en `linea`")

# Función para poblar la tabla `linea_vehiculo` (relación M:N entre linea y vehiculo)
def populate_linea_vehiculo(n):
    start_id_linea, end_id_linea = get_id_range("linea", "ID_Linea")
    start_id_vehiculo, end_id_vehiculo = get_id_range("Colectivo", "ID_Colectivo")

    for _ in range(n):
        while True:
            linea_id = random.randint(start_id_linea, end_id_linea)
            vehiculo_id = random.randint(start_id_vehiculo, end_id_vehiculo)
            fecha_asignacion = fake.date_between(start_date="-5y", end_date=datetime.strptime("2023-11-30", "%Y-%m-%d"))

            # Verificar si la combinación ya existe
            cur.execute("""
                SELECT 1 FROM transaccional.linea_vehiculo 
                WHERE linea_id = %s AND vehiculo_id = %s
            """, (linea_id, vehiculo_id))

            if not cur.fetchone():
                # Verificar si la fecha de asignación cumple con el intervalo
                cur.execute("""
                    SELECT fecha_de_asignacion FROM transaccional.linea_vehiculo 
                    WHERE vehiculo_id = %s
                    ORDER BY fecha_de_asignacion DESC
                    LIMIT 1
                """, (vehiculo_id,))

                last_assignment = cur.fetchone()
                if last_assignment:
                    last_assignment_date = last_assignment[0]
                    if (fecha_asignacion - last_assignment_date).days < 365 * 3:
                        continue  # Si la diferencia es menor a 3 años, continuar buscando

                # Si no existe o cumple con el intervalo, realizar la inserción y salir del bucle
                cur.execute("""
                    INSERT INTO transaccional.linea_vehiculo (linea_id, vehiculo_id, fecha_de_asignacion)
                    VALUES (%s, %s, %s)
                """, (linea_id, vehiculo_id, fecha_asignacion))
                break  # Salir del bucle while una vez insertado

    conn.commit()
    print(f"{n} registros insertados en `linea_vehiculo`")

# ------------------------------------POBLAR TABLAS------------------------------------
# Poblar todas las tablas con IDs manuales
#populate_ubicacion(1, 30)
#populate_parada(1, 30)
#populate_colectivo(1, 15) 
#populate_colectivo_parada(200)       # Genera 50 relaciones en `Colectivo_Parada`
#populate_clientes(101 , 75000)
""" populate_horarios(1, 20)    
populate_rutas(1, 10)      # Genera 10 rutas iniciales
populate_choferes(1, 5)  """   # Genera 5 choferes iniciales

# Poblar tablas intermedias y relaciones

#populate_asignacion_vehiculo(20)    # Genera 20 registros en `Asignacion_Vehiculo`
#populate_asignacion_conductor(10)   # Genera 10 registros en `Asignacion_Conductor`
#populate_horario_y_rutas(15)        # Genera 15 relaciones en `horario_y_rutas` """
#populate_linea(10)                  # Genera 10 líneas
populate_linea_vehiculo(20)         # Genera 20 relaciones en `linea_vehiculo`
# """
# Cerrar la conexión
cur.close()
conn.close()
