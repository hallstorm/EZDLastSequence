import mysql.connector
import pandas as pd

# Conectar con la base de datos
db = mysql.connector.connect(
    host="oltp-20211217073605262300000018.ca2xfs8jdjri.eu-west-1.rds.amazonaws.com",
    user="POSIS_PRD_OLTP_PBI_USER",
    password="hJlka8hlFZ7T9Qlh",
    database="oltp"
)

myEzdList = [   'POTIGIAN GOLOSINAS SACIF E I',
                'RAZ Y CIA. S.A.',
                'LA DOLCE S.R.L.',
                'DISTRIBUIDORA LA PRIMERA SRL',
                'CASA DANY S.R.L.',
                'DISTRIBUIDORA REDONDO S.A.C.I.F.A.',
                'CLAN S.R.L.',
                'MARIO DARIO ROMEO S.R.L.',
                'MELE Y CIA. S.A. COMERCIAL Y F',
                'MHFLIA S.R.L.',
                'CASA SILICARO S.A.',
                'S. TORRES Y CIA. S.A.',
                'CASA OSLE S.A.C.I.F.I. Y A.',
                'COSTANZO HNOS. S.R.L.',
                'DISTRIBUIDORA LOS GORDOS S.R.L',
                'TABACALERA NECOCHEA S.R.L.',
                'DICVA S.R.L.',
                'RIGLESA S.R.L.',
                'PEDRO BALESTRINO E HIJOS SACI Y A',
                'CASA MUÑIZ S.A.',
                'NANNI DISTRIBUIDORA S.A.',
                'PIANTONI HNOS. S.A.C.I.F.I.A',
                'LONDON SUPPLY S.A.C.I.F.I.',
                'JOSE V. PAOLETTI Y CIA. S.R.L.',
                'DISTRIBUIDORA JOR-GUS S.R.L.',
                'QUI-PAR S.R.L',
                'VENSAL HNOS. S.A',
                'ANTONIO L. CANDELERO S.R.L.',
                'BARALE HNOS. S.A.',
                'RODOLFO MANZANO SRL',
                'CLAUDIA GASTRICINI Y OTROS SRL',
                'JOSE OMAR DI ZEO SRL',
                'NESTOR GABRIEL BOSETTI S.R.L.',
                'JOSE NADAL E HIJOS S.R.L.',
                'MANUEL CASTILLO E HIJOS S.R.L.',
                'CIGARRERIA ROJO HNOS. S.A.',
                'GIBERAL S.A.',
                'FERRERO Y CIA. S.R.L.',
                'DELBECK S.R.L.',
                'DELIVERY S.R.L.',
                'CIMAR S.R.L.',
                'CASA TEIXEIRA S.A.',
                'SERRA S.R.L.',
                'PILFES S.R.L.',
                'MORENO S.R.L.',
                'FA Y FA S.R.L.',
                'MERCONOR S.R.L.',
                'PERELSTEIN DISTRIBUCIONES S.A.',
                'DISTRIBUIDORA ROMY S.R.L.',
                'ZAZA S.R.L.',
                'GALLO DISTRIBUCIONES S.A.',
                'MIGUEL ANGEL RAMONDA E HIJOS S.A.',
                'DON NESTOR S.A.',
                'TARDUCCI Y TORDINI S.R.L.',
                'CASA CHABELA CHACO S.R.L.',
                'MAXICO S.R.L.',
                'GOM-DIP S.R.L.',
                'DISTRIBUIDORA DEL CARMEN S.R.L',
                'PIANEZZOLA Y CIA. S.R.L.',
                'EZD S.R.L.',
                'EMPRENDIMIENTOS REGIONALES COMERCIA LES DEL OESTE S R L',
                'LONDON SUPPLY MINORISTA',
                'LAGOSPA SRL',
                'QUICK NESS S.R.L',
                'DISTRIBUIDORA MILENIUM S.A'
             ]

newDF = pd.DataFrame()  # creates a new dataframe that's empty

for ezd in myEzdList:
    # Crear un cursor para ejecutar las consultas SQL
    cursor = db.cursor()
    # Definir la consulta SQL SELECT que quieres ejecutar
    query = f"""select br.branchCode ,ingestiontimestamp, ingestionseqorapinumber, ezdsapezdlegalname
                from tbl_stg_ingestion_details id
                inner join tbl_ods_ezd on ingestion_ezdsk = ezdsk 
                inner join tbl_ods_branch br on br.branchsk = id.Ingestion_BranchSK
                where ezdsapezdlegalname = '{ezd}'
                and IngestionCreatedBy = 'LAMBDA.unzip_to_api_preprocessing' 
                and ingestioncurrentstatus = 'DATA_VALIDATED'
                and ingestiondbloadstatus = 'P'
                order by 2 desc
                LIMIT 1;"""

    # Ejecutar la consulta SQL SELECT
    cursor.execute(query)

    # Obtener los resultados de la consulta
    resultados = cursor.fetchall()

    branchCode = ''
    ingestionseqorapinumber = ''

    for resultado in resultados:
        branchCode = resultado[0]
        ingestionseqorapinumber = resultado[2]

    # Consultar los datos de la tabla que quieres exportar
    query = f"""SELECT  ezd.EZDSAPEZDLegalName,
                       ezd.EZDSAPEZDCode,
                       s.DocumentDate,
                       s.BranchCode,
                       s.EZDSequenceNumber,
                       pos.POSEZDPOSCode,
                       s.Product,
                       s.QuantityOfPacks,
                       s.SalesAmount
                FROM oltp.tbl_stg_sales s
                INNER JOIN tbl_ods_ezd ezd ON ezd.EZDSAPEZDCode = s.EZDCode
                INNER JOIN tbl_ods_pos pos ON pos.POSEZDPOSCode = s.POSCode
                WHERE ezd.EZDSAPEZDLegalName = '{ezd}'
                  AND s.BranchCode = '{branchCode}'
                  AND s.EZDSequenceNumber = '{ingestionseqorapinumber}'
                  AND pos.POS_EZDSK = ezd.EZDSK
                  AND pos.POS_TaxCategorySK = '104'
                  AND s.salesAmount IS NOT NULL
                  AND s.QuantityOfPacks > 0
                ORDER BY s.DocumentDate DESC ;"""

    dataFrameEZD = pd.read_sql(query, db)
    newDF = newDF.append(dataFrameEZD, ignore_index=True)  # ignoring index is optional

# Exportar los datos a un archivo de Excel
nombre_archivo = f"""totalPacksAmount.xlsx"""
newDF.to_excel(nombre_archivo, index=False)

# Cerrar la conexión con la base de datos
db.close()

print("Los datos se han exportado correctamente a un archivo de Excel.")
