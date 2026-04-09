FROM node:20-slim

ARG GIT_COMMIT=unknown
ARG GIT_REF=unknown
ENV GIT_COMMIT=${GIT_COMMIT}
ENV GIT_REF=${GIT_REF}

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

