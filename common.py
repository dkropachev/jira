"""Shared Jira utilities: config loading and JiraManager API client."""

import re
import sys

import requests
import yaml


def load_config(path):
    """Load YAML config file."""
    with open(path) as f:
        return yaml.safe_load(f)


class JiraManager:
    def __init__(self, base_url, user, token):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (user, token)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _get(self, path, params=None):
        resp = self.session.get(f"{self.base_url}{path}", params=params)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path, json_data):
        resp = self.session.post(f"{self.base_url}{path}", json=json_data)
        if not resp.ok:
            print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path, json_data):
        resp = self.session.put(f"{self.base_url}{path}", json=json_data)
        if not resp.ok:
            print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path):
        resp = self.session.delete(f"{self.base_url}{path}")
        if not resp.ok:
            print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return None

    # --- Field methods ---

    def get_fields(self):
        """Fetch all fields and return custom fields."""
        fields = self._get("/rest/api/3/field")
        return [f for f in fields if f.get("custom", False)]

    def find_field(self, name):
        """Find a custom field by name (case-insensitive)."""
        fields = self.get_fields()
        name_lower = name.lower()
        for f in fields:
            if f["name"].lower() == name_lower:
                return f
        return None

    def get_contexts(self, field_id):
        """Get all contexts for a custom field."""
        data = self._get(f"/rest/api/3/field/{field_id}/context")
        return data.get("values", [])

    def get_options(self, field_id, context_id):
        """Get all options for a field context, handling pagination."""
        options = []
        start_at = 0
        while True:
            data = self._get(
                f"/rest/api/3/field/{field_id}/context/{context_id}/option",
                params={"startAt": start_at, "maxResults": 1000},
            )
            options.extend(data.get("values", []))
            if data.get("isLast", True):
                break
            start_at += len(data.get("values", []))
        return options

    def add_option(self, field_id, context_id, value):
        """Add a new option to a field context."""
        return self._post(
            f"/rest/api/3/field/{field_id}/context/{context_id}/option",
            {"options": [{"value": value}]},
        )

    def find_options(self, field_id, context_id, pattern):
        """Find options whose value matches a regexp or glob pattern.

        If the pattern is not valid regex, it is treated as a glob
        (fnmatch) pattern and converted to regex.

        Returns a list of matching option dicts.
        """
        import fnmatch
        options = self.get_options(field_id, context_id)
        try:
            regex = re.compile(pattern)
        except re.error:
            regex = re.compile(fnmatch.translate(pattern), re.IGNORECASE)
        return [opt for opt in options if regex.search(opt["value"])]

    def update_option(self, field_id, context_id, option_id, new_value):
        """Rename an existing option."""
        return self._put(
            f"/rest/api/3/field/{field_id}/context/{context_id}/option",
            {"options": [{"id": option_id, "value": new_value}]},
        )

    # --- Issue methods ---

    def search_issues(self, jql, max_results=50, start_at=0):
        """Search issues using JQL."""
        return self._get("/rest/api/3/search", params={
            "jql": jql,
            "maxResults": max_results,
            "startAt": start_at,
        })

    def get_issue(self, key):
        """Get a single issue by key."""
        return self._get(f"/rest/api/3/issue/{key}")

    def delete_issue(self, key):
        """Delete a single issue by key."""
        return self._delete(f"/rest/api/3/issue/{key}")
