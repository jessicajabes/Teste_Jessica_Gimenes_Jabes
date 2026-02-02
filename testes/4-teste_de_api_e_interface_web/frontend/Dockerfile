FROM node:18-alpine

WORKDIR /app

# Copiar package files
COPY package*.json ./

# Instalar dependências
RUN npm install

# Copiar código
COPY . .

# Expor porta
EXPOSE 5173

# Comando para iniciar em modo dev
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
