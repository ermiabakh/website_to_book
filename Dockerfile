FROM python:3.10-slim-buster

WORKDIR /app

# Install Node.js (required by Playwright install-deps)
RUN apt-get update && apt-get install -y curl
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
RUN apt-get install -y nodejs

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser dependencies
RUN playwright install-deps

# Install Chromium browser for Playwright
RUN playwright install

COPY . .

# Expose the port Quart app runs on (default is 9999, but Hypercorn might use 5000)
EXPOSE 9999

# Set environment variables if needed (e.g., for Quart configuration)
# ENV QUART_DEBUG=1

# Command to run the Quart application using Hypercorn
CMD ["hypercorn", "--bind", "0.0.0.0:9999", "deep2:app"]