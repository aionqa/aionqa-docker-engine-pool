FROM node:20-slim

# AWS CLI no longer needed - ECR repositories are public
# Install curl for health checks (if needed)
RUN apt-get update && \
    apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY src/ ./src/

CMD ["node", "src/engine.js"]

