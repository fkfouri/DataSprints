# TESTE ENGENHEIRO DE DADOS - DataSprints

## SOBRE ESTE REPOSITORIO

Este material é parte do processo de seleção para Engenheiros de Dados na DataSprints. 

Para este desafio foi disponibilizado 3 conjuntos de dados contento dados sobre corridas de Taxi na cidade de New York ocorridas entre 2009 e 2012, dados de empresas de Taxi e Mapa de prefixos e os tipos de pagamento.

Este repositório contém a resolução dos Quesistos Mínimos e dos Quesistos bônus.


## Instruções para Reprodução de Análises
Para reprodução local recomenda-se o uso do Jupyter Notebook, escolha o arquivo desafio.ipynb.

 
### Processamento em Cluster
O arquivo "/cluster/AWS.ipynb" contém uma sequencia de configuração dos servidos da Amazon (S3, Group Resource, EMR) para a criação de um cluster Spark On-Demand formado por um Master e dois Slaves.

A execução deste script requer que o Engenheiro de Dados possua uma conta ativa na AWS e que possua configurado localmente o AWS CLI. 

Esta execução pode causar COBRANÇA da AWS pelo uso dos serviços. É necessario observar o tempo de duração dos serviços para não causar prejuízo. Para efeito de referencia, durante os meus testes, a configuração e a utilização do cluster levou no melhor tempo 8 minutos e no pior tempo cerca de 20 minutos.

Foi criado um step no cluster da EMR para a execução do Script Python. A entrada de dados, o script pyspark e a saída parquet todos foram armazenados no bucket S3.

