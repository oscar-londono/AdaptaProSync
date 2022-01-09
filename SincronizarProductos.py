import includes.LogClass as Log

import mysql.connector as MySql
import configparser
import sys
import os

# Instanciamos el Objeto de manejo de archivo log
Log = Log.Log(os.path.splitext(os.path.basename(sys.argv[0]))[0])
Log.Informacion("Inicio de la sincronizacion de productos")

# Leemos el archivo de la configuración
Config = configparser.ConfigParser()
Config.read('ini\config.ini')

# Conexion a la Base de Datos Origen
try:
 ConexionOrigen = MySql.connect(host=Config.get('SERVER_SOURCE','BD_HOST'),
                                user=Config.get('SERVER_SOURCE','BD_USER'),
                                password=Config.get('SERVER_SOURCE','BD_PASSWORD'),
                                database=Config.get('SERVER_SOURCE','BD_NAME'))
except MySql.Error as Error:
 Log.Error("No es posible realizar la conexion de origen ({0})" . format(Error))
 sys.exit()

# Conexion a la Base de Datos Destino
try:
 ConexionDestino = MySql.connect(host=Config.get('SERVER_DESTINATION','BD_HOST'),
                                 user=Config.get('SERVER_DESTINATION','BD_USER'),
                                 password=Config.get('SERVER_DESTINATION','BD_PASSWORD'),
                                 database=Config.get('SERVER_DESTINATION','BD_NAME'))
except MySql.Error as Error:
 Log.Error("No es posible realizar la conexion de destino ({0})" . format(Error))
 sys.exit()

# Actualizar Marcas de Producto
try:
 CursorDpMarcasOrigen = ConexionOrigen.cursor()
 CursorDpMarcasOrigen.execute("SELECT * FROM DPMARCAS")
 ResultadoDpMarcasOrigen = CursorDpMarcasOrigen.fetchall()
except MySql.Error as Error:
 Log.Error("No es posible realizar la consulta de DpMarcas origen ({0})" . format(Error))
 sys.exit()

for DpMarca in ResultadoDpMarcasOrigen:
 try:
  CursorDpMarcasDestino = ConexionDestino.cursor()
  CursorDpMarcasDestino.execute("SELECT MAR_CODIGO FROM DPMARCAS WHERE (MAR_CODIGO='" + DpMarca[1] + "')")
  ResultadoDpMarcasDestino = CursorDpMarcasDestino.fetchall()
 except MySql.Error as Error:
  Log.Error("No es posible realizar la consulta de DpMarcas destino: " + DpMarca[1] + " ({0})" . format(Error))

 Campos = ""
 Valores = ""
 CamposValores = ""
 for Campo, Valor in zip(CursorDpMarcasOrigen.description,DpMarca):
  Comilla = ("" if (Campo[1]==246) else "'")
  Campos = Campos + Campo[0] + ","
  Valores = Valores + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","
  if ((Campo[0]!="MAR_CODIGO")):
   CamposValores = CamposValores + Campo[0] + '=' + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","

 Campos = Campos[:-1]
 Valores = Valores[:-1]
 CamposValores = CamposValores[:-1]

 CursorDpMarcasActualizar = ConexionDestino.cursor()
 if (CursorDpMarcasDestino.rowcount==0):
  try:
   CursorDpMarcasActualizar.execute("INSERT INTO DPMARCAS (" + Campos + ") VALUES (" + Valores + ")")
   ConexionDestino.commit()
   Log.Informacion("Marca de Producto de codigo " + DpMarca[1] + " agregada satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Agregando la Marca de Producto: " + DpMarca[1] + " ({0})" . format(Error))
 else:
  try:
   CursorDpMarcasActualizar.execute("UPDATE DPMARCAS SET " + CamposValores + " WHERE (MAR_CODIGO='" + DpMarca[1] + "')")
   ConexionDestino.commit()
   Log.Informacion("Marca de Producto de codigo " + DpMarca[1] + " modificada satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Modificando la Marca de Producto: " + DpMarca[1] + " ({0})" . format(Error))

 CursorDpMarcasActualizar.close()
 CursorDpMarcasDestino.close()
CursorDpMarcasOrigen.close()

# Actualizar Grupos de Producto
try:
 CursorDpGruposOrigen = ConexionOrigen.cursor()
 CursorDpGruposOrigen.execute("SELECT * FROM DPGRU")
 ResultadoDpGruposOrigen = CursorDpGruposOrigen.fetchall()
except MySql.Error as Error:
 Log.Error("No es posible realizar la consulta de DpGrupos origen ({0})" . format(Error))
 sys.exit()

for DpGrupo in ResultadoDpGruposOrigen:
 try:
  CursorDpGruposDestino = ConexionDestino.cursor()
  CursorDpGruposDestino.execute("SELECT GRU_CODIGO FROM DPGRU WHERE (GRU_CODIGO='" + DpGrupo[0] + "')")
  ResultadoDpGruposDestino = CursorDpGruposDestino.fetchall()
 except MySql.Error as Error:
  Log.Error("No es posible realizar la consulta de DpGrupos destino: " + DpGrupo[0] + " ({0})" . format(Error))

 Campos = ""
 Valores = ""
 CamposValores = ""
 for Campo, Valor in zip(CursorDpGruposOrigen.description,DpGrupo):
  Comilla = ("" if (Campo[1]==246) else "'")
  Campos = Campos + Campo[0] + ","
  Valores = Valores + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","
  if ((Campo[0]!="GRU_CODIGO")):
   CamposValores = CamposValores + Campo[0] + '=' + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","

 Campos = Campos[:-1]
 Valores = Valores[:-1]
 CamposValores = CamposValores[:-1]

 CursorDpGruposActualizar = ConexionDestino.cursor()
 if (CursorDpGruposDestino.rowcount==0):
  try:
   CursorDpGruposActualizar.execute("INSERT INTO DPGRU (" + Campos + ") VALUES (" + Valores + ")")
   ConexionDestino.commit()
   Log.Informacion("Grupo de Producto de codigo " + DpGrupo[0] + " agregado satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Agregando el Grupo de Producto: " + DpGrupo[0] + " ({0})" . format(Error))
 else:
  try:
   CursorDpGruposActualizar.execute("UPDATE DPGRU SET " + CamposValores + " WHERE (GRU_CODIGO='" + DpGrupo[0] + "')")
   ConexionDestino.commit()
   Log.Informacion("Grupo de Producto de codigo " + DpGrupo[0] + " modificado satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Modificando el Grupo de Producto: " + DpGrupo[0] + " ({0})" . format(Error))

 CursorDpGruposActualizar.close()
 CursorDpGruposDestino.close()
CursorDpGruposOrigen.close()

# Actualizar Sub-Grupos de Producto
try:
 CursorDpSubGruposOrigen = ConexionOrigen.cursor()
 CursorDpSubGruposOrigen.execute("SELECT * FROM DPSUBGRU")
 ResultadoDpSubGruposOrigen = CursorDpSubGruposOrigen.fetchall()
except MySql.Error as Error:
 Log.Error("No es posible realizar la consulta de DpSubGrupos origen ({0})" . format(Error))
 sys.exit()

for DpSubGrupo in ResultadoDpSubGruposOrigen:
 try:
  CursorDpSubGruposDestino = ConexionDestino.cursor()
  CursorDpSubGruposDestino.execute("SELECT SUB_CODIGO FROM DPSUBGRU WHERE ((SUB_CODGRU='" + DpSubGrupo[0] + "') AND (SUB_CODIGO='" + DpSubGrupo[1] + "'))")
  ResultadoDpSubGruposDestino = CursorDpSubGruposDestino.fetchall()
 except MySql.Error as Error:
  Log.Error("No es posible realizar la consulta de DpSubGrupos destino: " + DpSubGrupo[0] + "-" + DpSubGrupo[1] + " ({0})" . format(Error))

 Campos = ""
 Valores = ""
 CamposValores = ""
 for Campo, Valor in zip(CursorDpSubGruposOrigen.description,DpSubGrupo):
  Comilla = ("" if (Campo[1]==246) else "'")
  Campos = Campos + Campo[0] + ","
  Valores = Valores + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","
  if ((Campo[0]!="SUB_CODGRU") and (Campo[0]!="SUB_CODIGO")):
   CamposValores = CamposValores + Campo[0] + '=' + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","

 Campos = Campos[:-1]
 Valores = Valores[:-1]
 CamposValores = CamposValores[:-1]

 CursorDpSubGruposActualizar = ConexionDestino.cursor()
 if (CursorDpSubGruposDestino.rowcount==0):
  try:
   CursorDpSubGruposActualizar.execute("INSERT INTO DPSUBGRU (" + Campos + ") VALUES (" + Valores + ")")
   ConexionDestino.commit()
   Log.Informacion("Sub-Grupo de Producto de codigo " + DpSubGrupo[0] + "-" + DpSubGrupo[1] + " agregado satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Agregando el Sub-Grupo de Producto: " + DpSubGrupo[0] + "-" + DpSubGrupo[1] + " ({0})" . format(Error))
 else:
  try:
   CursorDpSubGruposActualizar.execute("UPDATE DPSUBGRU SET " + CamposValores + " WHERE ((SUB_CODGRU='" + DpSubGrupo[0] + "') AND (SUB_CODIGO='" + DpSubGrupo[1] + "'))")
   ConexionDestino.commit()
   Log.Informacion("Sub-Grupo de Producto de codigo " + DpSubGrupo[0] + "-" + DpSubGrupo[1] + " modificado satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Modificando el Sub-Grupo de Producto: " + DpSubGrupo[0] + "-" + DpSubGrupo[1] + " ({0})" . format(Error))

 CursorDpSubGruposActualizar.close()
 CursorDpSubGruposDestino.close()
CursorDpSubGruposOrigen.close()

# Actualizar Productos
try:
 CursorDpProductosOrigen = ConexionOrigen.cursor()
 CursorDpProductosOrigen.execute("SELECT * FROM DPINV")
 ResultadoDpProductosOrigen = CursorDpProductosOrigen.fetchall()
except MySql.Error as Error:
 Log.Error("No es posible realizar la consulta de DpProductos origen ({0})" . format(Error))
 sys.exit()

for DpProducto in ResultadoDpProductosOrigen:
 try:
  CursorDpProductosDestino = ConexionDestino.cursor()
  CursorDpProductosDestino.execute("SELECT INV_CODIGO FROM DPINV WHERE (INV_CODIGO='" + DpProducto[27] + "')")
  ResultadoDpProductosDestino = CursorDpProductosDestino.fetchall()
 except MySql.Error as Error:
  Log.Error("No es posible realizar la consulta de DpProductos destino: " + DpProducto[27] + " ({0})" . format(Error))

 Campos = ""
 Valores = ""
 CamposValores = ""
 for Campo, Valor in zip(CursorDpProductosOrigen.description,DpProducto):
  Comilla = ("" if (Campo[1]==246) else "'")
  Campos = Campos + Campo[0] + ","
  Valores = Valores + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","
  if ((Campo[0]!="INV_CODIGO") and (Campo[0]!="INV_EXIMIN") and (Campo[0]!="INV_EXIMAX") and (Campo[0]!="INV_FCHVEN") and (Campo[0]!="INV_NUMMER") and (Campo[0]!="INV_NUMME2") and (Campo[0]!="INV_PORUTI")):
   CamposValores = CamposValores + Campo[0] + '=' + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","

 Campos = Campos[:-1]
 Valores = Valores[:-1]
 CamposValores = CamposValores[:-1]

 CursorDpProductosActualizar = ConexionDestino.cursor()
 CursorDpProductosActualizar2 = ConexionDestino.cursor()
 if (CursorDpProductosDestino.rowcount==0):
  try:
   CursorDpProductosActualizar.execute("INSERT INTO DPINV (" + Campos + ") VALUES (" + Valores + ")")
   ConexionDestino.commit()
   Log.Informacion("Producto de codigo " + DpProducto[27] + " agregado satisfactoriamente");

   CursorDpProductosActualizar2.execute("INSERT INTO DPINVMED (IME_CODIGO,IME_UNDMED,IME_CANTID,IME_PRESEN,IME_PESO,IME_COMPRA,IME_VENTA,IME_VOLUME,IME_SIGNO,IME_MEDPRE) VALUES ('" + DpProducto[27] + "','Unidad',1.000,'Unidad',0.000,'S','S',0.000,'*',1)")
   ConexionDestino.commit()
   Log.Informacion("Unidad de Medida de Producto de codigo " + DpProducto[27] + " agregada satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Agregando el Producto: " + DpProducto[27] + " ({0})" . format(Error))
 else:
  try:
   CursorDpProductosActualizar.execute("UPDATE DPINV SET " + CamposValores + " WHERE (INV_CODIGO='" + DpProducto[27] + "')")
   ConexionDestino.commit()
   Log.Informacion("Producto de codigo " + DpProducto[27] + " modificado satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Modificando el Producto: " + DpProducto[27] + " ({0})" . format(Error))

 CursorDpProductosActualizar2.close()
 CursorDpProductosActualizar.close()
 CursorDpProductosDestino.close()
CursorDpProductosOrigen.close()

# Actualizar Equivalencias de Producto
try:
 CursorDpEquivalenciasOrigen = ConexionOrigen.cursor()
 CursorDpEquivalenciasOrigen.execute("SELECT * FROM DPEQUIV")
 ResultadoDpEquivalenciasOrigen = CursorDpEquivalenciasOrigen.fetchall()
except MySql.Error as Error:
 Log.Error("No es posible realizar la consulta de DpEquivalencias origen ({0})" . format(Error))
 sys.exit()

for DpEquivalencia in ResultadoDpEquivalenciasOrigen:
 try:
  CursorDpEquivalenciasDestino = ConexionDestino.cursor()
  CursorDpEquivalenciasDestino.execute("SELECT EQUI_BARRA FROM DPEQUIV WHERE ((EQUI_CODIG='" + DpEquivalencia[1] + "') AND (EQUI_MED='" + DpEquivalencia[2] + "'))")
  ResultadoDpEquivalenciasDestino = CursorDpEquivalenciasDestino.fetchall()
 except MySql.Error as Error:
  Log.Error("No es posible realizar la consulta de DpEquivalencias destino: " + DpEquivalencia[1] + "-" + DpEquivalencia[2] + " ({0})" . format(Error))

 Campos = ""
 Valores = ""
 CamposValores = ""
 for Campo, Valor in zip(CursorDpEquivalenciasOrigen.description,DpEquivalencia):
  Comilla = ("" if (Campo[1]==246) else "'")
  Campos = Campos + Campo[0] + ","
  Valores = Valores + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","
  if ((Campo[0]!="EQUI_CODIG") and (Campo[0]!="EQUI_MED")):
   CamposValores = CamposValores + Campo[0] + '=' + Comilla + (str(Valor) if ((Campo[1]==10) or (Campo[1]==246)) else ("" if (Valor is None) else Valor)) + Comilla + ","

 Campos = Campos[:-1]
 Valores = Valores[:-1]
 CamposValores = CamposValores[:-1]

 CursorDpEquivalenciasActualizar = ConexionDestino.cursor()
 if (CursorDpEquivalenciasDestino.rowcount==0):
  try:
   CursorDpEquivalenciasActualizar.execute("INSERT INTO DPEQUIV (" + Campos + ") VALUES (" + Valores + ")")
   ConexionDestino.commit()
   Log.Informacion("Equivalencia de Producto de codigo " + DpEquivalencia[1] + "-" + DpEquivalencia[2] + " agregada satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Agregando la Equivalencia de Producto: " + DpEquivalencia[1] + "-" + DpEquivalencia[2] + " ({0})" . format(Error))
 else:
  try:
   CursorDpEquivalenciasActualizar.execute("UPDATE DPEQUIV SET " + CamposValores + " WHERE ((EQUI_CODIG='" + DpEquivalencia[1] + "') AND (EQUI_MED='" + DpEquivalencia[2] + "'))")
   ConexionDestino.commit()
   Log.Informacion("Equivalencia de Producto de codigo " + DpEquivalencia[1] + "-" + DpEquivalencia[2] + " modificada satisfactoriamente");
  except MySql.Error as Error:
   ConexionDestino.rollback()
   Log.Error("Error Modificando la Equivalencia de Producto: " + DpEquivalencia[1] + "-" + DpEquivalencia[2] + " ({0})" . format(Error))

 CursorDpEquivalenciasActualizar.close()
 CursorDpEquivalenciasDestino.close()
CursorDpEquivalenciasOrigen.close()

ConexionDestino.close()
ConexionOrigen.close()
Log.Informacion("Fin de la sincronizacion de productos")