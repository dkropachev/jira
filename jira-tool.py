#!/usr/bin/env python3
"""Universal Jira CLI tool.

Usage:
    # Field operations
    jira-tool.py field list-fields
    jira-tool.py field list-options --field "Scylla Components"
    jira-tool.py field find-option --field "Scylla Components" "Driver.*"
    jira-tool.py field add-option --field "Scylla Components" --value "Driver - gocqlx"
    jira-tool.py field rename-option --field "Scylla Components" --old "Old" --new "New"

    # Issue operations
    jira-tool.py issue list
    jira-tool.py issue list --project DRIVER
    jira-tool.py issue list --jql "project = DRIVER AND status = Open"
    jira-tool.py issue list --all
    jira-tool.py issue get DRIVER-123
    jira-tool.py issue delete DRIVER-123 --yes

Requires env var: USER_AND_KEY_FOR_JIRA_AUTOMATION (format: user:token)
"""

import argparse
import os
import sys

from common import JiraManager, load_config


def handle_field_command(args, mgr):
    """Handle all field subcommands."""
    if args.field_action == "list-fields":
        fields = mgr.get_fields()
        for f in sorted(fields, key=lambda x: x["name"]):
            schema_type = f.get("schema", {}).get("type", "?")
            print(f"  {f['name']:40s}  id={f['id']:30s}  type={schema_type}")
        return

    # All other field actions require --field
    if not args.field:
        print("Error: --field is required for this action", file=sys.stderr)
        sys.exit(1)

    field = mgr.find_field(args.field)
    if not field:
        print(f"Error: field '{args.field}' not found. Use 'field list-fields' to see available fields.",
              file=sys.stderr)
        sys.exit(1)

    field_id = field["id"]
    print(f"Field: {field['name']} (id={field_id})", file=sys.stderr)

    contexts = mgr.get_contexts(field_id)
    if not contexts:
        print(f"Error: no contexts found for field '{field['name']}'", file=sys.stderr)
        sys.exit(1)

    if len(contexts) > 1:
        print(f"Found {len(contexts)} contexts:", file=sys.stderr)
        for ctx in contexts:
            print(f"  id={ctx['id']}  name={ctx.get('name', 'N/A')}", file=sys.stderr)
        print("Using the first context.", file=sys.stderr)

    context_id = contexts[0]["id"]

    if args.field_action == "list-options":
        options = mgr.get_options(field_id, context_id)
        print(f"Options for '{field['name']}' (context {context_id}):")
        for opt in sorted(options, key=lambda x: x["value"]):
            disabled = " (disabled)" if opt.get("disabled", False) else ""
            print(f"  {opt['value']}{disabled}")
        print(f"\nTotal: {len(options)} options")

    elif args.field_action == "find-option":
        matches = mgr.find_options(field_id, context_id, args.pattern)
        if not matches:
            print(f"No options matching '{args.pattern}'")
            return
        for opt in sorted(matches, key=lambda x: x["value"]):
            disabled = " (disabled)" if opt.get("disabled", False) else ""
            print(f"  {opt['value']}{disabled}")
        print(f"\nMatched: {len(matches)} options")

    elif args.field_action == "add-option":
        existing = mgr.get_options(field_id, context_id)
        for opt in existing:
            if opt["value"].lower() == args.value.lower():
                print(f"Option '{opt['value']}' already exists (id={opt['id']})")
                return

        result = mgr.add_option(field_id, context_id, args.value)
        new_options = result.get("options", [])
        if new_options:
            print(f"Created option: '{new_options[0]['value']}' (id={new_options[0]['id']})")
        else:
            print("Option created successfully")

    elif args.field_action == "rename-option":
        existing = mgr.get_options(field_id, context_id)
        match = None
        for opt in existing:
            if opt["value"] == args.old:
                match = opt
                break
        if not match:
            print(f"Error: option '{args.old}' not found. Use 'field list-options' to see available options.",
                  file=sys.stderr)
            sys.exit(1)

        result = mgr.update_option(field_id, context_id, match["id"], args.new)
        updated = result.get("options", [])
        if updated:
            print(f"Renamed: '{args.old}' -> '{updated[0]['value']}' (id={updated[0]['id']})")
        else:
            print("Renamed successfully")


def handle_issue_command(args, mgr, config):
    """Handle all issue subcommands."""
    if args.issue_action == "list":
        if args.jql:
            jql = args.jql
        else:
            project = args.project or config.get("jira_project")
            if not project:
                print("Error: --project is required (or set jira_project in config)", file=sys.stderr)
                sys.exit(1)
            jql = f"project = {project} ORDER BY created DESC"

        max_results = args.max_results or 50

        if args.all:
            issues = []
            start_at = 0
            while True:
                data = mgr.search_issues(jql, max_results=100, start_at=start_at)
                batch = data.get("issues", [])
                issues.extend(batch)
                if start_at + len(batch) >= data.get("total", 0):
                    break
                start_at += len(batch)
        else:
            data = mgr.search_issues(jql, max_results=max_results)
            issues = data.get("issues", [])

        if not issues:
            print("No issues found.")
            return

        # Print table
        print(f"{'KEY':<16} {'STATUS':<16} {'ASSIGNEE':<24} SUMMARY")
        print("-" * 100)
        for issue in issues:
            key = issue["key"]
            fields = issue["fields"]
            status = fields.get("status", {}).get("name", "")
            assignee_obj = fields.get("assignee")
            assignee = assignee_obj.get("displayName", "") if assignee_obj else "Unassigned"
            summary = fields.get("summary", "")
            print(f"{key:<16} {status:<16} {assignee:<24} {summary}")

        print(f"\nTotal: {len(issues)} issues")

    elif args.issue_action == "get":
        issue = mgr.get_issue(args.key)
        fields = issue["fields"]

        assignee_obj = fields.get("assignee")
        assignee = assignee_obj.get("displayName", "") if assignee_obj else "Unassigned"
        reporter_obj = fields.get("reporter")
        reporter = reporter_obj.get("displayName", "") if reporter_obj else "Unknown"

        print(f"Key:         {issue['key']}")
        print(f"Summary:     {fields.get('summary', '')}")
        print(f"Status:      {fields.get('status', {}).get('name', '')}")
        print(f"Type:        {fields.get('issuetype', {}).get('name', '')}")
        print(f"Assignee:    {assignee}")
        print(f"Reporter:    {reporter}")
        print(f"Created:     {fields.get('created', '')}")
        print(f"Updated:     {fields.get('updated', '')}")

        description = fields.get("description")
        if description:
            print(f"\nDescription:")
            # Jira API v3 returns ADF; render plain text from paragraphs
            if isinstance(description, dict):
                for block in description.get("content", []):
                    if block.get("type") == "paragraph":
                        text = "".join(
                            node.get("text", "") for node in block.get("content", [])
                        )
                        print(f"  {text}")
                    elif block.get("type") == "codeBlock":
                        text = "".join(
                            node.get("text", "") for node in block.get("content", [])
                        )
                        print(f"  ```\n  {text}\n  ```")
            else:
                print(f"  {description}")

    elif args.issue_action == "delete":
        print(f"Deleting issue {args.key}...")
        mgr.delete_issue(args.key)
        print(f"Issue {args.key} deleted.")


def main():
    parser = argparse.ArgumentParser(
        description="Universal Jira CLI tool"
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command group")

    # --- field command ---
    field_parser = subparsers.add_parser("field", help="Manage custom field options")
    field_sub = field_parser.add_subparsers(dest="field_action", help="Field action")

    # field list-fields
    field_sub.add_parser("list-fields", help="List all custom fields")

    # field list-options
    lo_parser = field_sub.add_parser("list-options", help="List options for a field")
    lo_parser.add_argument("--field", required=True, help="Custom field name")

    # field add-option
    ao_parser = field_sub.add_parser("add-option", help="Add a new option to a field")
    ao_parser.add_argument("--field", required=True, help="Custom field name")
    ao_parser.add_argument("--value", required=True, help="Option value to add")

    # field find-option
    fo_parser = field_sub.add_parser("find-option", help="Find options matching a regexp")
    fo_parser.add_argument("--field", required=True, help="Custom field name")
    fo_parser.add_argument("pattern", help="Regular expression to match against option values")

    # field rename-option
    ro_parser = field_sub.add_parser("rename-option", help="Rename an existing option")
    ro_parser.add_argument("--field", required=True, help="Custom field name")
    ro_parser.add_argument("--old", required=True, help="Current option value")
    ro_parser.add_argument("--new", required=True, help="New option value")

    # --- issue command ---
    issue_parser = subparsers.add_parser("issue", help="Manage issues")
    issue_sub = issue_parser.add_subparsers(dest="issue_action", help="Issue action")

    # issue list
    il_parser = issue_sub.add_parser("list", help="List issues")
    il_parser.add_argument("--project", help="Project key (default: jira_project from config)")
    il_parser.add_argument("--jql", help="Custom JQL query")
    il_parser.add_argument("--all", action="store_true", help="Paginate through all results")
    il_parser.add_argument("--max-results", type=int, help="Max results per page (default: 50)")

    # issue get
    ig_parser = issue_sub.add_parser("get", help="Get issue details")
    ig_parser.add_argument("key", help="Issue key (e.g. DRIVER-123)")

    # issue delete
    id_parser = issue_sub.add_parser("delete", help="Delete an issue")
    id_parser.add_argument("key", help="Issue key (e.g. DRIVER-123)")
    id_parser.add_argument("--yes", required=True, action="store_true",
                           help="Confirm deletion (required)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "field" and not args.field_action:
        field_parser.print_help()
        sys.exit(1)

    if args.command == "issue" and not args.issue_action:
        issue_parser.print_help()
        sys.exit(1)

    config = load_config(args.config)

    user_and_key = os.environ.get("USER_AND_KEY_FOR_JIRA_AUTOMATION")
    if not user_and_key or ":" not in user_and_key:
        print("Error: USER_AND_KEY_FOR_JIRA_AUTOMATION env var required (format: user:token)", file=sys.stderr)
        sys.exit(1)
    jira_user, jira_token = user_and_key.split(":", 1)

    mgr = JiraManager(config["jira"]["url"], jira_user, jira_token)

    if args.command == "field":
        handle_field_command(args, mgr)
    elif args.command == "issue":
        handle_issue_command(args, mgr, config)


if __name__ == "__main__":
    main()
