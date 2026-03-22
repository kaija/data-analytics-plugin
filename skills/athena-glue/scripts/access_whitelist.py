"""Access Whitelist Enforcement for Athena/Glue Skill.

Loads a whitelist configuration from JSON and validates that SQL queries
only reference authorized databases and tables. Follows a deny-by-default
posture: all access is denied unless explicitly whitelisted.

Requirements: 3.9, 3.10, 3.11, 3.12
"""

import json
import os
import re
from dataclasses import dataclass, field


class WhitelistConfigError(Exception):
    """Raised when the whitelist configuration file is missing or malformed.

    Attributes:
        message: Human-readable description of the configuration error.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict:
        return {
            "error": "whitelist_config_error",
            "message": self.message,
        }


@dataclass
class UnauthorizedResource:
    """A database/table pair that is not in the whitelist."""

    database: str
    table: str | None = None


@dataclass
class WhitelistValidationResult:
    """Result of validating a SQL query against the access whitelist."""

    authorized: bool
    unauthorized_resources: list[UnauthorizedResource] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "authorized": self.authorized,
            "unauthorized_resources": [
                {"database": r.database, "table": r.table}
                for r in self.unauthorized_resources
            ],
        }


class AccessWhitelist:
    """Enforces database/table access control via a JSON whitelist config.

    The whitelist file must conform to the schema:
    {
        "version": "1.0",
        "allowed": [
            {"database": "<name>", "tables": ["<table>", ...] or ["*"]}
        ]
    }

    A table value of "*" means all tables in that database are allowed.
    """

    def __init__(
        self,
        config_path: str = "skills/athena-glue/assets/access-whitelist.json",
    ) -> None:
        """Load and validate the whitelist config.

        Args:
            config_path: Path to the whitelist JSON file.

        Raises:
            WhitelistConfigError: If the file is missing, contains invalid
                JSON, or is missing required keys.
        """
        self._allowed: dict[str, set[str]] = {}
        self._load(config_path)

    def _load(self, config_path: str) -> None:
        if not os.path.isfile(config_path):
            raise WhitelistConfigError(
                f"Access whitelist not found at {config_path}"
            )

        try:
            with open(config_path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            raise WhitelistConfigError(
                f"Access whitelist contains invalid JSON: {exc}"
            )

        if not isinstance(data, dict):
            raise WhitelistConfigError(
                "Access whitelist must be a JSON object"
            )

        if "version" not in data:
            raise WhitelistConfigError(
                "Access whitelist missing required key: 'version'"
            )

        if "allowed" not in data:
            raise WhitelistConfigError(
                "Access whitelist missing required key: 'allowed'"
            )

        if not isinstance(data["allowed"], list):
            raise WhitelistConfigError(
                "Access whitelist 'allowed' must be an array"
            )

        for i, entry in enumerate(data["allowed"]):
            if not isinstance(entry, dict):
                raise WhitelistConfigError(
                    f"Access whitelist entry {i} must be an object"
                )
            if "database" not in entry:
                raise WhitelistConfigError(
                    f"Access whitelist entry {i} missing required key: 'database'"
                )
            if "tables" not in entry:
                raise WhitelistConfigError(
                    f"Access whitelist entry {i} missing required key: 'tables'"
                )
            if not isinstance(entry["tables"], list):
                raise WhitelistConfigError(
                    f"Access whitelist entry {i} 'tables' must be an array"
                )

            db = entry["database"]
            tables = set(entry["tables"])
            self._allowed[db] = tables

    def is_authorized(self, database: str, table: str | None = None) -> bool:
        """Check if a database/table is in the whitelist.

        Args:
            database: The database name to check.
            table: Optional table name. If None, checks database-level access.

        Returns:
            True if the resource is whitelisted, False otherwise.
        """
        if database not in self._allowed:
            return False

        if table is None:
            return True

        allowed_tables = self._allowed[database]
        return "*" in allowed_tables or table in allowed_tables

    def validate_query(self, sql: str) -> WhitelistValidationResult:
        """Parse SQL to extract referenced tables and validate against whitelist.

        Uses regex-based extraction to find table references in FROM and JOIN
        clauses. Supports both ``database.table`` and bare ``table`` formats.

        Args:
            sql: The SQL query string.

        Returns:
            WhitelistValidationResult with authorized flag and list of
            unauthorized resources.
        """
        references = self._extract_table_references(sql)
        unauthorized: list[UnauthorizedResource] = []

        for database, table in references:
            if not self.is_authorized(database, table):
                unauthorized.append(UnauthorizedResource(database=database, table=table))

        return WhitelistValidationResult(
            authorized=len(unauthorized) == 0,
            unauthorized_resources=unauthorized,
        )

    @staticmethod
    def _extract_table_references(sql: str) -> list[tuple[str, str | None]]:
        """Extract table references from SQL using regex.

        Looks for table references after FROM and JOIN keywords.
        Handles ``database.table`` and bare ``table`` patterns.

        Returns:
            List of (database, table) tuples. For bare table names,
            database is the table name and table is None.
        """
        references: list[tuple[str, str | None]] = []
        seen: set[tuple[str, str | None]] = set()

        # Pattern matches FROM/JOIN followed by optional whitespace and a
        # table reference. Table reference can be:
        #   database.table  or  just_table_name
        # We stop at whitespace, comma, parenthesis, or semicolon.
        pattern = re.compile(
            r"""
            (?:FROM|JOIN)        # FROM or JOIN keyword
            \s+                  # required whitespace
            (`?)                 # optional backtick quote (group 1)
            ([\w]+)              # first identifier — database or table (group 2)
            \1                   # matching closing backtick
            (?:                  # optional .table part
                \.
                (`?)             # optional backtick quote (group 3)
                ([\w]+)          # second identifier — table (group 4)
                \3               # matching closing backtick
            )?
            """,
            re.IGNORECASE | re.VERBOSE,
        )

        for match in pattern.finditer(sql):
            first = match.group(2)
            second = match.group(4)

            if second:
                ref = (first, second)
            else:
                ref = (first, None)

            if ref not in seen:
                seen.add(ref)
                references.append(ref)

        return references
