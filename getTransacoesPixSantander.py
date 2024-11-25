#WESLEY 23.11.24
import requests
import json
import crcmod
import os
import sys
import http.client
import urllib.parse
import ssl
from datetime import datetime, timedelta
import cx_Oracle
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
import random
import string

#PARAMETROS
iDMENOS = 1
try:    
    iDMENOS = int(sys.argv[1])
except:
    pass
iDATA_EXTRACAO = datetime.now() + timedelta(days=-iDMENOS)

#LOG
sys.path.append("R:/DEPARTAMENTO/TECNOLOGIA/FUNCIONAL/Projetos/libpydavo" if os.name == 'nt' else "/usr/davo/libpydavo")
from logging_config import setup_logger #log padrao
logger = setup_logger(app_name=os.path.basename(__file__).replace('.py', ''), project_name="pix")

#VARIAVEIS
iIMPORTAORACLE = True
iNOME_INSTITUICAO = "SANTANDER"
iTIPO_TRAN = "pix"
logger.debug(f"os.name: {os.name}")
if os.name == 'nt': #windows    
    iCERTFILECRT = "R:/DEPARTAMENTO/TECNOLOGIA/FUNCIONAL/Projetos/PIX/SANTANDER/A1DAVOPEMcertificate.crt"
    iKEYFILE = "R:/DEPARTAMENTO/TECNOLOGIA/FUNCIONAL/Projetos/PIX/SANTANDER/A1DAVOPEMprivate.key"
else:
    os.environ['ORACLE_HOME'] = "/usr/lib/oracle/19.6/client64"
    iCERTFILECRT = "/usr/davo/PIX/Keys/A1DAVOPEMcertificate.crt"
    iKEYFILE = "/usr/davo/PIX/Keys/A1DAVOPEMprivate.key"

#AMBIENTE
iAMBIENTESANTANDER = "prd"
logger.debug(f"iAMBIENTESANTANDER: {iAMBIENTESANTANDER}")
if iAMBIENTESANTANDER == "prd":
    iURL = "trust-pix.santander.com.br"
    iCLIENTSECRET = "e5QqaKU7NMJbn0LD"
    iCLIENTID = "lWiRkbb8Wo4NWyZDGANys7Bcfofa32FY"

try:    
    myCONNORA = cx_Oracle.connect('davo/d4v0@davoprd') #conexao com o Oracle
    myCONNORA.autocommit = True
    curORA = myCONNORA.cursor() #execucoes Oracle
    try:
        curORA.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ',.' ")
        curORA.execute("alter session set nls_date_format = 'DD/MM/YYYY'")    
    except cx_Oracle.DatabaseError as e_sql: 
        print("Erro : " + str(e_sql))
except:
    print("Erro ao comunicar com Oracle em")
    exit()
    pass

def getToken(iCERTFILECRT, iKEYFILE, iURL, iCLIENTID, iCLIENTSECRET):    
    url = iURL
    endpoint = "/oauth/token?grant_type=client_credentials"          
    payload = {
                'client_id': str(iCLIENTID),
                'client_secret': str(iCLIENTSECRET),
                'grant_type': 'client_credentials'
            }
    encoded_payload = urllib.parse.urlencode(payload)    
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)    
    conn = http.client.HTTPSConnection(url, context=context)    
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)  
    context.load_cert_chain(certfile=iCERTFILECRT, keyfile=iKEYFILE)    
    conn = http.client.HTTPSConnection(url, context=context)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }    
    logger.debug(f"endpoint: {endpoint}")
    conn.request("POST", endpoint, headers=headers, body=encoded_payload)
    response = conn.getresponse()
    status_code = response.status
    data = response.read()
    logger.debug(f"status_code: {status_code}")
    if status_code == 200:
        response_json = json.loads(data.decode('utf-8'))
        access_token = response_json['access_token']
    else:
        access_token = ""
        logger.debug(f"response.text: {response.text}")
    return access_token



######################  CONSULTA RECEBIMENTOS PIX
def gravaTRANSACAO(iNOME_INSTITUICAO, iTIPO_TRAN, iDATA_TRAN, iENDTOENDID, iTXID, iCHAVE, iVALOR, iPAGADOR, iVAL_DEVOLUCAO, iCLIENTID, iPAGE):
    logger.info(f"Funcao, grava transacao no banco de dados")
    iSTATUS_CONCILIACAO = 0     

    iQUERY = (f"""
                        BEGIN
                        BEGIN
                            INSERT INTO davo.conc_bank_pix
                                        (
                                                    instituicao,
                                                    tipo_extracao,
                                                    data_extracao,
                                                    data_transacao,
                                                    endtoendid,
                                                    txid,
                                                    chave,
                                                    valor,
                                                    infopagador,
                                                    devolucao_valor,
                                                    status_conc,
                                                    cliente_id,
                                                    page
                                        )
                            SELECT '{iNOME_INSTITUICAO}',
                                '{iTIPO_TRAN}',
                                sysdate,
                                To_date('{iDATA_TRAN}', 'yyyy-mm-dd hh24:mi:ss'),
                                '{iENDTOENDID}',
                                '{iTXID}',
                                '{iCHAVE}',
                                {iVALOR},
                                '{iPAGADOR}',
                                {iVAL_DEVOLUCAO},
                                {iSTATUS_CONCILIACAO},
                                '{str(iCLIENTID[0:49])}',
                                {iPAGE}
                            FROM   dual
                            WHERE  NOT EXISTS
                                (
                                        SELECT p1.instituicao
                                        FROM   davo.conc_bank_pix p1
                                        WHERE  p1.instituicao = '{iNOME_INSTITUICAO}'
                                        AND    p1.txid = '{iTXID}'
                                        AND    p1.valor = {iVALOR} ) ;
                            
                            exception
                        WHEN dup_val_on_index THEN
                            dbms_output.put_line('Duplicate value on an index. endtoendid: {iENDTOENDID}');
                        END;
                        MERGE
                        INTO         davo.conc_bank_pix pix
                        using        (
                                            SELECT p1.instituicao,
                                                    p1.data_transacao,
                                                    p1.valor,
                                                    p1.devolucao_valor,
                                                    p1.endtoendid
                                            FROM   davo.conc_bank_pix p1
                                            WHERE  p1.instituicao = '{iNOME_INSTITUICAO}'
                                            AND    p1.txid = '{iTXID}') a
                        ON (
                                                    a.instituicao = pix.instituicao
                                    AND          a.endtoendid = pix.endtoendid
                                    AND          (
                                                                a.devolucao_valor != {iVAL_DEVOLUCAO}
                                                    OR           a.valor != {iVALOR} ) )
                        WHEN matched THEN
                        UPDATE
                        SET    pix.devolucao_valor = {iVAL_DEVOLUCAO},
                                pix.valor = {iVALOR},
                                pix.status_conc = 0;

                        END;
              """)
    if iIMPORTAORACLE == False:
        return ""
    try:
        curORA.execute(iQUERY)
    except cx_Oracle.DatabaseError as e_sql: 
        logger.error(f"{e_sql}")
        logger.error(f"{iQUERY}")

def trataJsonPixRecebidos(iJSON, iDATA_EXTRACAO, iURL, iPAGE, iCERTFILECRT, iKEYFILE, iCLIENTID, iCLIENTSECRET):
    iPAGINATUAL = iJSON["parametros"]["paginacao"]["paginaAtual"]
    iCONTAREG = 0
    try:
        for itens in iJSON["pix"]:
            data_hora = datetime.strptime(itens['horario'], "%Y-%m-%dT%H:%M:%SZ")
            formato_desejado = "%d/%m/%Y"
            data_hora_subtrai_X_horas = data_hora - timedelta(hours=0)
            iDATA_TRAN = str(data_hora_subtrai_X_horas)[0:10] + " " + str(data_hora_subtrai_X_horas)[11:19]

            try:
                iENDTOENDID = itens['endToEndId']
            except:
                iENDTOENDID = ""
                pass

            try:
                iTXID = itens['txid']
            except:
                iTXID = ""
                pass
            
            try:            
                iCHAVE = itens['chave']
            except:
                iCHAVE = ""
                pass
            try:
                iVALOR = itens['valor']
            except:
                iVALOR = 0
                pass

            iPAGADOR = ""
            try:
                iPAGADOR = itens['infoPagador']
                ### QBG RR
                iPAGADOR = iPAGADOR.replace ("'", "") 
            except:
                pass

            iVAL_DEVOLUCAO = 0
            try:
                if "devolucoes" in itens:
                    for dev in itens["devolucoes"]:
                        if dev["status"] == "DEVOLVIDO":
                            iVALDEV_API = float(dev["valor"])
                            iVAL_DEVOLUCAO += iVALDEV_API
                        else:
                            logger.debug(f"status de devolucao nao concluida, nao considerara: {dev['status']}")
            except:
                pass
            gravaTRANSACAO(iNOME_INSTITUICAO, iTIPO_TRAN, iDATA_TRAN, iENDTOENDID, iTXID, iCHAVE, iVALOR, iPAGADOR, iVAL_DEVOLUCAO, iCLIENTID, iPAGE)    
            iCONTAREG += 1
    except Exception as e:
        print(f"{e}")
    print(f"    Total de registros: {iCONTAREG}")
    if iCONTAREG >0: getPixRecebidos(iDATA_EXTRACAO, iURL, iPAGE+1, iCERTFILECRT, iKEYFILE, iCLIENTID, iCLIENTSECRET)    


def getPixRecebidos(iDATA_EXTRACAO, iURL, iPAGE, iCERTFILECRT, iKEYFILE, iCLIENTID, iCLIENTSECRET):
    url = iURL
    conn = http.client.HTTPSConnection(url, cert_file=iCERTFILECRT, key_file=iKEYFILE)
    iHORA_FIM = "23:59:59"
    if datetime.now().strftime('%Y_%m_%d') == iDATA_EXTRACAO.strftime('%Y_%m_%d'): #SE FOR IGUAL HOJE ENTAO HORA FIM = HORA ATUAL, SENAO 23:59
        iHORA_FIM = str(datetime.now().strftime('%H:%M:%S'))
    endpoint = "/api/v1/pix?inicio=" + str(iDATA_EXTRACAO.strftime('%Y-%m-%d')) + "T00:00:01Z&fim=" + str(iDATA_EXTRACAO.strftime('%Y-%m-%d') ) + "T" + str(iHORA_FIM) + "Z&paginacao.paginaAtual=" + str(iPAGE)
    print(f"client_id: {iCLIENTID}      endpoint: {endpoint}")
    iTOKEN = getToken(iCERTFILECRT, iKEYFILE, iURL, iCLIENTID, iCLIENTSECRET)
    headers = {
                'Authorization': 'Bearer ' + str(iTOKEN)
         }
    logger.debug(f"url: {url}")
    logger.debug(f"endpoint: {endpoint}")
    conn.request("GET", endpoint, headers=headers)
    response = conn.getresponse()
    data = response.read()
    status_code = response.status
    if status_code == 200:
        trataJsonPixRecebidos(json.loads(data), iDATA_EXTRACAO, iURL, iPAGE, iCERTFILECRT, iKEYFILE, iCLIENTID, iCLIENTSECRET)
    else:
        logger.debug(f"{data}")
        logger.debug(f"Abortando, sem mais ocorrencias!")
        return ""





########################### GERA COBRANCA
def calcular_crc16_ccitt_false(texto):
    crc16_func = crcmod.predefined.mkPredefinedCrcFun('crc-ccitt-false')
    texto_bytes = texto.encode('utf-8')
    crc_result = crc16_func(texto_bytes)    
    # Retornando o valor em hexadecimal, com 4 digitos
    return f"{crc_result:04X}"

def gerar_string_personalizada():
    hexadecimais = ''.join(random.choices(string.hexdigits.lower(), k=32))
    letra = random.choice(string.ascii_lowercase)
    return hexadecimais + letra

def geraCopiaCola(iLOCATION, iVALOR):
    """    
    00020101021226850014br.gov.bcb.pix2563pix.santander.com.br/qr/v2/da2fc726-fde3-41a0-8642-e803f944567352040000530398654040.105802BR5923D'AVO SUPERMERCADO LTDA6008SAOPAULO62070503***6304DEA6
    """
    iSTRINGCOPIACOLA = ""
    iSTRINGCOPIACOLA += "000201"
    iSTRINGCOPIACOLA += "010212"
    iSTRINGCOPIACOLA += "26"
    iGRUPO = f"0014br.gov.bcb.pix25{len(iLOCATION)}{iLOCATION}"
    iSTRINGCOPIACOLA += f"{len(iGRUPO)}"
    iSTRINGCOPIACOLA += iGRUPO
    iSTRINGCOPIACOLA += "5204"
    iSTRINGCOPIACOLA += "00005303986"
    iSTRINGCOPIACOLA += "54"
    iSTRINGCOPIACOLA += str(len(str(iVALOR))).zfill(2)
    iSTRINGCOPIACOLA += str(iVALOR)
    iSTRINGCOPIACOLA += "5802BR"
    iSTRINGCOPIACOLA += "59"
    iNOMERECEBEDOR = "D'AVO SUPERMERCADO LTDA"
    iSTRINGCOPIACOLA += str(len(iNOMERECEBEDOR))
    iSTRINGCOPIACOLA += iNOMERECEBEDOR
    iSTRINGCOPIACOLA += "6008SAOPAULO"
    iSTRINGCOPIACOLA += "62070503***"
    iSTRINGCOPIACOLA += "6304"
    iCRC16 = calcular_crc16_ccitt_false(iSTRINGCOPIACOLA)
    iSTRINGCOPIACOLA += str(iCRC16)
    return iSTRINGCOPIACOLA

def criaCob(iCERTFILECRT, iKEYFILE, iURL, iCLIENTID, iCLIENTSECRET, iTXID, iSEGUNDOSEXPIRA, iVALOR, iCHAVERECEBEDOR):    
    url = iURL
    conn = http.client.HTTPSConnection(url, cert_file=iCERTFILECRT, key_file=iKEYFILE)
    endpoint = f"/api/v1/cob/{iTXID}"
    iTOKEN = getToken(iCERTFILECRT, iKEYFILE, iURL, iCLIENTID, iCLIENTSECRET)
    headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + str(iTOKEN)
         }
    payload = {
                "calendario": {
                    "expiracao": iSEGUNDOSEXPIRA
                },
                "valor": {
                    "original": str(iVALOR)
                },
                "chave": str(iCHAVERECEBEDOR)
                } 
    json_payload = json.dumps(payload) 
    logger.debug(f"url: {url}")
    logger.debug(f"endpoint: {endpoint}")
    conn.request("PUT", endpoint, headers=headers, body=json_payload)
    response = conn.getresponse()
    data = response.read()
    status_code = response.status
    logger.debug(f"status_code: {status_code}")
    if status_code == 201:
        iJSON = json.loads((data.decode('utf-8')))
        return geraCopiaCola(iJSON["location"], iJSON["valor"]["original"])
    else:
        logger.debug(f"{data}")
    return ""



#################### ACTIONS
def geraNovaCobranca(iTXID="", iSEGUNDOSEXPIRA=0, iVALOR=0):
    if iTXID == "": iTXID = gerar_string_personalizada()
    if iSEGUNDOSEXPIRA == 0: iSEGUNDOSEXPIRA = 3600
    iCHAVERECEBEDOR = "43f29896-366c-4b36-a010-f2de140799dd"
    i = criaCob(iCERTFILECRT, iKEYFILE, iURL, iCLIENTID, iCLIENTSECRET, iTXID, iSEGUNDOSEXPIRA, iVALOR, iCHAVERECEBEDOR)
    return i

def consultaRecebimentosPix(iDATA_EXTRACAO):
    getPixRecebidos(iDATA_EXTRACAO, iURL, 0, iCERTFILECRT, iKEYFILE, iCLIENTID, iCLIENTSECRET)


##GERAR NOVA COBRANCA
#iVALORCOBRANCA = 1.55
#pix_copia_cola = geraNovaCobranca("", 0, iVALORCOBRANCA)
#print(f"pix cpia e cola: {pix_copia_cola}")

#CONSULTA COBRANCA PIX
consultaRecebimentosPix(iDATA_EXTRACAO)

try:
    myCONNORA.close()
except:
    pass
try:
    curORA.close()
except:
    pass