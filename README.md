# Industrial Process Analytics System

A real-time data processing and analytics system for manufacturing environments that integrates equipment controllers, product scanners, and conveyor systems to provide comprehensive production insights.

##  Overview

This system continuously monitors and analyzes production line data by:
- Correlating conveyor requests with product scans and equipment events
- Calculating real-time process statistics (cycle times, efficiency, wait times)
- Generating analytics by date, operator, product, and process
- Maintaining complete audit trails of production cycles

##  Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Equipment     │     │     Scanner      │     │    Conveyor     │
│   Controller    │────▶│     Database     │◀────│    Requests     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Data Processor      │
                    │  - Correlation Logic  │
                    │  - State Management   │
                    └───────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Analytics Engine     │
                    │  - Daily Stats        │
                    │  - Operator Stats     │
                    │  - Product Stats      │
                    │  - Process Stats      │
                    └───────────────────────┘
```

##  Key Features

### Real-Time Processing
- **Event-driven architecture** with Redis pub/sub support
- **Automatic state recovery** from last processed timestamp
- **Hot-reload capability** for development (runner.py)
- **Continuous monitoring** with configurable cycle intervals

### Data Correlation
- **Multi-source integration**: Links equipment events, scanner data, and conveyor timestamps
- **Smart time-window matching**: Finds corresponding codes within configurable time ranges
- **Cycle completion verification**: Ensures process integrity before advancing

### Analytics Modules

#### Daily Statistics
- Total processes, average/max/min cycle times
- Standard deviation and efficiency metrics
- Wait time analysis (conveyor to equipment start)

#### Operator Performance
- Processes per hour and efficiency by operator
- Time statistics across date ranges
- Product variety handled per operator

#### Product Analysis
- Production time vs total time breakdown
- Throughput (products per hour)
- Process completion rates by product code

#### Process Details
- Side-by-side processing times (Side A, Side B)
- Phase-by-phase time tracking
- Process status decoding (bitwise flags)

## 🛠️ Technical Stack

- **Language**: Python 3.x
- **Database**: MariaDB
- **Caching**: Redis (optional)
- **Key Libraries**:
  - `mariadb` - Database connectivity
  - `redis` - Event streaming
  - `watchdog` - File change detection
  - Standard library (`datetime`, `logging`, `contextlib`)

##  Project Structure

```
├── main.py                      # Entry point and system orchestrator
├── runner.py                    # Hot-reload development server
├── config/
│   ├── credentials.py          # Database and Redis configuration
│   └── logging_config.py       # Centralized logging setup
├── database/
│   ├── connection_manager.py   # Database connection pooling
│   ├── query_executor.py       # Clean query interface with context managers
│   ├── schema_manager.py       # Database schema initialization
│   ├── state_manager.py        # System state persistence
│   └── descriptions_dict.py    # Product code catalog
├── processing/
│   ├── data_processor.py       # Core correlation logic
│   ├── equipment_data_handler.py # Equipment data operations
│   └── process_decoder.py      # Status bitfield decoder
└── analytics/
    ├── common_functions.py     # Shared utilities
    ├── daily.py               # Daily statistics
    ├── operators.py           # Operator analytics
    ├── products.py            # Product analytics
    └── processes.py           # Detailed process analytics
```

##  Getting Started

### Prerequisites

```bash
# Python 3.8+
python --version

# MariaDB 10.5+
mysql --version

# Redis (optional)
redis-cli --version
```

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/industrial-process-analytics-system.git
cd industrial-process-analytics-system

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install mariadb redis watchdog
```

### Configuration

Create a `.env` file with your database credentials:

```bash
# Equipment Database
DB_EQUIPMENT_HOST=localhost
DB_EQUIPMENT_USER=your_user
DB_EQUIPMENT_PASS=your_password
DB_EQUIPMENT_NAME=equipment_db
DB_EQUIPMENT_PORT=3306

# Scanner Database
DB_CODE_HOST=localhost
DB_CODE_USER=your_user
DB_CODE_PASS=your_password
DB_CODE_NAME=scanner_db
DB_CODE_PORT=3306

# Analytics Database
DB_COMBINED_HOST=localhost
DB_COMBINED_USER=your_user
DB_COMBINED_PASS=your_password
DB_COMBINED_NAME=analytics_db
DB_COMBINED_PORT=3307

# Redis (optional)
USE_REDIS=false
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_CHANNEL=default:channel
```

### Running the System

```bash
# Production mode (standard execution)
python main.py

# Development mode (with hot-reload)
python runner.py
```

##  Database Schema

The system expects the following tables:

**Source Tables:**
- `tb_equipment_records` - Equipment controller status logs
- `tb_product_scanner` - Product scan events
- `tb_conveyor_requests` - Conveyor timing marks

**Analytics Tables:**
- `tb_combined_data` - Correlated event data
- `tb_daily_statistics` - Daily aggregated metrics
- `tb_operator_statistics` - Performance by operator
- `tb_product_statistics` - Metrics by product code
- `tb_process_statistics` - Detailed process breakdown
- `tb_processing_state` - System checkpoint state

##  Key Design Patterns

### Context Manager Pattern
```python
# Clean connection handling with automatic commit/rollback
with query_executor.connection('analytics') as (conn, cursor):
    cursor.execute(query1, params1)
    cursor.execute(query2, params2)
    # Auto-commits on success
```

### State Management
- Persistent checkpoint system prevents duplicate processing
- Automatic recovery from last successful timestamp
- Handles system restarts gracefully

### Bitwise Status Decoding
```python
# Equipment controller sends 6-bit status field
# decode_status_current() finds highest active bit (current state)
# decode_status_complete() returns all executed states
```

##  Performance Considerations

- **Batch operations**: Uses `executemany()` for bulk inserts
- **Connection pooling**: Reuses connections across cycles
- **Time-range optimization**: Limits equipment queries to relevant windows
- **Upsert logic**: `ON DUPLICATE KEY UPDATE` prevents conflicts


##  Logging

Logs are written to `./logs/System_process.log` with:
- Rotating file handler (10MB max, 5 backups)
- Color-coded console output
- INFO level for file, WARNING+ for console

## 🤝 Contributing

This is a portfolio project demonstrating production system architecture. Feel free to:
- Fork and adapt for your own use cases
- Submit issues for questions or discussions
- Suggest improvements via pull requests

## 📄 License

MIT License - See LICENSE file for details

## 🎓 Learning Highlights

This project demonstrates:
- **Clean architecture** with separation of concerns
- **Production-ready** error handling and logging
- **Scalable design** for continuous data processing
- **Database optimization** with CTEs and batch operations
- **State management** for fault tolerance

## 📧 Contact

For questions or collaborations, reach out via [your-contact-method]

---

**Note**: This is an anonymized version of a production system. Database schemas and exact business logic have been abstracted for portfolio purposes.
