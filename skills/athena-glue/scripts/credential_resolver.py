"""AWS Credential Resolver with priority-ordered discovery.

Resolves AWS credentials automatically using the following priority order:
1. IAM instance role (EC2/ECS/Lambda metadata endpoint)
2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
3. Shared credential file (~/.aws/credentials)
4. AWS config file (~/.aws/config)
5. Named profile (from AWS_PROFILE env var or config)

Requirements: 3.7, 3.8
"""

import os
from dataclasses import dataclass, field

import botocore.session


@dataclass
class AWSCredentials:
    """Resolved AWS credentials."""

    access_key: str
    secret_key: str
    token: str | None = None
    method: str = ""


class CredentialResolutionError(Exception):
    """Raised when all credential discovery methods fail.

    Attributes:
        attempted_methods: List of dicts with 'method' and 'result' keys
            describing each failed discovery attempt.
    """

    def __init__(self, attempted_methods: list[dict[str, str]]) -> None:
        self.attempted_methods = attempted_methods
        super().__init__(
            f"Failed to resolve AWS credentials. "
            f"Attempted methods: {[m['method'] for m in attempted_methods]}"
        )

    def to_dict(self) -> dict:
        return {
            "error": "credential_resolution_failed",
            "message": "Failed to resolve AWS credentials",
            "attempted_methods": self.attempted_methods,
        }


class CredentialResolver:
    """Resolves AWS credentials by trying discovery methods in priority order.

    DISCOVERY_ORDER defines the sequence of credential sources to attempt.
    The first successful source wins. If all fail, a CredentialResolutionError
    is raised listing every attempted method and its failure reason.
    """

    DISCOVERY_ORDER = [
        "iam_role",
        "environment_variables",
        "shared_credential_file",
        "aws_config_file",
        "named_profile",
    ]

    def resolve(self) -> AWSCredentials:
        """Attempt credential discovery in priority order.

        Returns:
            AWSCredentials on success.

        Raises:
            CredentialResolutionError: When no discovery method yields valid
                credentials. The exception carries an ``attempted_methods``
                list with one entry per method.
        """
        attempted: list[dict[str, str]] = []

        for method_name in self.DISCOVERY_ORDER:
            resolver_fn = getattr(self, f"_try_{method_name}")
            try:
                creds = resolver_fn()
                if creds is not None:
                    return creds
                attempted.append(
                    {"method": method_name, "result": "no credentials found"}
                )
            except Exception as exc:
                attempted.append({"method": method_name, "result": str(exc)})

        raise CredentialResolutionError(attempted)

    # ------------------------------------------------------------------
    # Individual discovery methods
    # ------------------------------------------------------------------

    def _try_iam_role(self) -> AWSCredentials | None:
        """Try to obtain credentials from the instance metadata service.

        This covers EC2 instance roles, ECS task roles, and Lambda
        execution roles via the botocore InstanceMetadataProvider.
        """
        from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher

        provider = InstanceMetadataProvider(
            iam_role_fetcher=InstanceMetadataFetcher(timeout=1, num_attempts=1)
        )
        creds = provider.load()
        if creds is None:
            return None
        frozen = creds.get_frozen_credentials()
        if not frozen.access_key:
            return None
        return AWSCredentials(
            access_key=frozen.access_key,
            secret_key=frozen.secret_key,
            token=frozen.token or None,
            method="iam_role",
        )

    def _try_environment_variables(self) -> AWSCredentials | None:
        """Try to read credentials from environment variables."""
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not access_key or not secret_key:
            return None
        return AWSCredentials(
            access_key=access_key,
            secret_key=secret_key,
            token=os.environ.get("AWS_SESSION_TOKEN") or None,
            method="environment_variables",
        )

    def _try_shared_credential_file(self) -> AWSCredentials | None:
        """Try to load credentials from ~/.aws/credentials."""
        from botocore.credentials import SharedCredentialProvider

        session = botocore.session.Session()
        provider = SharedCredentialProvider(
            creds_filename=os.path.expanduser("~/.aws/credentials"),
        )
        creds = provider.load()
        if creds is None:
            return None
        return AWSCredentials(
            access_key=creds.access_key,
            secret_key=creds.secret_key,
            token=creds.token or None,
            method="shared_credential_file",
        )

    def _try_aws_config_file(self) -> AWSCredentials | None:
        """Try to load credentials from ~/.aws/config."""
        from botocore.credentials import ConfigProvider

        session = botocore.session.Session()
        config_mapping = session.full_config.get("profiles", {})
        provider = ConfigProvider(
            config_filename=os.path.expanduser("~/.aws/config"),
            profile_name=session.profile or "default",
            config_parser=session._config_parser,
        )
        creds = provider.load()
        if creds is None:
            return None
        return AWSCredentials(
            access_key=creds.access_key,
            secret_key=creds.secret_key,
            token=creds.token or None,
            method="aws_config_file",
        )

    def _try_named_profile(self) -> AWSCredentials | None:
        """Try to load credentials from a named profile.

        Uses the AWS_PROFILE environment variable if set, otherwise
        falls back to the 'default' profile via a full botocore session
        credential chain scoped to that profile.
        """
        profile_name = os.environ.get("AWS_PROFILE", "default")
        session = botocore.session.Session(profile=profile_name)
        try:
            creds = session.get_credentials()
        except Exception:
            return None
        if creds is None:
            return None
        frozen = creds.get_frozen_credentials()
        if not frozen.access_key:
            return None
        return AWSCredentials(
            access_key=frozen.access_key,
            secret_key=frozen.secret_key,
            token=frozen.token or None,
            method="named_profile",
        )
