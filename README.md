# MQ - Message Queue Implementation

A lightweight message queue implementation inspired by RabbitMQ, built with Java and Spring Boot.

## Tech Stack

- **Java**: 17
- **Framework**: Spring Boot 3.5.8
- **Persistence**: MyBatis + SQLite
- **Build Tool**: Maven

## Project Structure

```
mq/
├── src/                  # Source code
├── docs/                 # Documentation
│   ├── course.pdf       # Course material
│   ├── notes.md         # Learning notes
│   ├── assets/          # PDF extracted images
│   ├── markdown-images/ # Markdown embedded images
│   └── 板书/            # Course board notes
├── pom.xml              # Maven configuration
└── README.md            # This file
```

## Quick Start

### Prerequisites

- JDK 17 or higher
- Maven 3.6+

### Build

```bash
# Build the project
./mvnw clean package

# Run tests
./mvnw test
```

### Run

```bash
# Run the application
./mvnw spring-boot:run
```

## Documentation

For detailed learning notes and implementation details, see:
- [Learning Notes](docs/notes.md) - Comprehensive study notes
- [Course Material](docs/course.pdf) - Original course PDF

## Development

This is a learning project implementing core message queue concepts including:
- Message persistence
- Queue management
- Publisher/Consumer pattern
- Message routing

## License

Educational project - free to use and modify.

---

**Note**: This is a learning project created for educational purposes.
