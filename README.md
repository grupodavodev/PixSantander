pinheirocfc@gmail.com

# PixSantander
Operação com PIX Santander - API

# Cadastro Desenvolvedor
Necessário cadastro de desenvolvedor em: https://developer.santander.com.br/login
Posteriormente a empresa precisa se logar (com as credenciais da empresa) e fazer um convite para esta conta de desenvolvedor.

# Cadastro da aplicação
Para quem possui certificado A1, necessario separar .CRT e .KEY:
No linux ->
Transformando o arquivo A1 em .PEM
openssl pkcs12 -in {caminho do certificado A1 -out {caminho que gravara o arquivo PEM, sem extensao do arquivo}.pem -nodes
quebrando o certificado em KEY e CRT ->
openssl pkey -in {meu arquivo .pem} -out {caminho que gravara o arquivo KEY, sem extensao do arquivo}.key
openssl x509 -in {meu arquivo .pem} -out {caminho que gravara o arquivo CRT, sem extensao do arquivo}.crt
Em seguida criar a aplicação dentro da pagina de desenvolvedor

Assim que submetido / aprovado (o que demora em média 5 minutos) o client_id / client_secret será liberado no portal

