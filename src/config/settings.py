"""
Configuration settings for the data model transformation project.
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "default"
    timeout: int = 300  # 5 minutes timeout

    @property
    def connection_params(self) -> Dict[str, Any]:
        """Return connection parameters for clickhouse-connect."""
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "database": self.database,
            "send_receive_timeout": self.timeout
        }


# Database configurations
SOURCE_DB = DatabaseConfig(
    host="rccp301-34a.iad6.prod.conviva.com"
)

TARGET_DB = DatabaseConfig(
    host="rccp301-34a.iad6.prod.conviva.com"
)

# Table configurations
SOURCE_TABLE = "default.eco_cross_page_flow_pt1m_local_20251008_3cust"
TARGET_TABLE = "default.eco_cross_page_preagg_pt1m_3cust"

# Key mapping file path
KEY_MAPPING_FILE = "output/mappings/key_mapping_mul_cust.json"

# Columns to exclude from target table
EXCLUDED_COLUMNS = {
    "flowId",
    "flowStartTimeMs",
    # Tag groups are excluded as per requirements
    "tagGroup1", "tagGroup2", "tagGroup3", "tagGroup4", "tagGroup5",
    "tagGroup6", "tagGroup7", "tagGroup8", "tagGroup9", "tagGroup10",
    "tagGroup11", "tagGroup12", "tagGroup13", "tagGroup14", "tagGroup15"
}

# Standard columns to preserve (metadata)
STANDARD_COLUMNS = [
    "timestampMs", "customerId", "clientId", "sessionId", "inSession",
    "userSessionId", "inUserSession", "platform", "platformSubcategory",
    "appName", "appBuild", "appVersion", "browserName", "browserVersion",
    "userId", "deviceManufacturer", "deviceMarketingName", "deviceModel",
    "deviceHardwareType", "deviceName", "deviceCategory", "deviceOperatingSystem",
    "deviceOperatingSystemVersion", "deviceOperatingSystemFamily", "country",
    "state", "city", "countryIso", "sub1Iso", "sub2Iso", "cityGid", "dma",
    "postalCode", "isp", "netSpeed", "sensorVersion", "appType", "asn",
    "timezoneOffsetMins", "connType", "watermarkMs", "partitionId", "retentionDate"
]

# Customer configurations
# CUSTOMERS = [1960181009, 1960181845, 1960183305, 1960183601]
CUSTOMERS = [1960181009, 1960181845, 1960183305]

CUSTOMER_NAMES = {
    1960181009: "Intigral",
    1960181845: "SlingTV",
    1960183305: "NFL",
    1960183601: "Zee"
}

# Date range configurations for benchmarking
# Using unified date for validation: October 8, 2025
UNIFIED_DATE_START = "2025-10-08 00:00:00"
UNIFIED_DATE_END = "2025-10-09 00:00:00"

# Transformation date (extracted from UNIFIED_DATE_START for YYYY-MM-DD format)
TRANSFORMATION_DATE = UNIFIED_DATE_START.split()[0]  # "2025-10-08"

# Aggregation window used by benchmarking query generation (in hours)
AGG_WINDOW_HOURS = 6

# Query dimensions for GROUP BY clauses
DIMENSIONS = ["deviceCategory", "countryIso", "platform"]