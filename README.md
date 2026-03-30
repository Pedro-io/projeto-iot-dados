## Setup

Para executar este projeto, é necessário ter o Docker instalado em sua máquina. Certifique-se de que o Docker está funcionando corretamente antes de prosseguir.

### Passos via Terminal

1. Acesse a pasta do Docker:
   ```
   cd infra/docker
   ```

2. Copie o arquivo de exemplo para a pasta do Docker:
   ```
   cp ../../data/sample/* .
   ```

3. Execute o Docker Compose para iniciar os serviços:
   ```
   docker-compose up
   ```