FROM python:3.10

# Defina o diretório de trabalho
WORKDIR /app

# Copie o arquivo requirements.txt para o contêiner
COPY requirements.txt .

# Instale as dependências do requirements.txt
RUN pip install -r requirements.txt

# Copie o restante dos arquivos da aplicação
COPY . .

# Exponha a porta 5000
EXPOSE 5000

# Comando padrão para executar o aplicativo
CMD ["tail", "-f", "/dev/null"]

