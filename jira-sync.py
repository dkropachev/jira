#!/usr/bin/env python3
"""GitHub-to-Jira Epic Migration Tool.

Scans GitHub repos for epics and sub-issues via GraphQL API,
detects already-migrated issues, and produces a YAML migration plan.
Optionally executes the plan to create Jira issues.

Also detects missing 'Scylla Components' field values from config
and includes them as create_field_option actions at the top of the plan.
"""

import argparse
import os
import re
import sys

import requests
import yaml

from common import JiraManager, load_config

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"


def load_and_validate_config(path):
    """Load and validate YAML config."""
    config = load_config(path)
    if "jira" not in config or "url" not in config["jira"]:
        raise ValueError("Config must contain jira.url")
    if "repos" not in config or not config["repos"]:
        raise ValueError("Config must contain at least one repo entry")
    if "jira_project" not in config:
        raise ValueError("Config must contain jira_project")
    for repo in config["repos"]:
        for key in ("github", "jira_prefix", "scylla_components"):
            if key not in repo:
                raise ValueError(f"Repo entry missing required key: {key}")
    return config


def github_graphql(query, variables, token):
    """Execute a GraphQL query against the GitHub API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "GraphQL-Features": "sub_issues, issue_types",
    }
    resp = requests.post(
        GITHUB_GRAPHQL_URL,
        json={"query": query, "variables": variables},
        headers=headers,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data["data"]


def fetch_epics(owner, repo, token):
    """Fetch all epics in a repo using GitHub search GraphQL."""
    query = """
    query($searchQuery: String!, $cursor: String) {
      search(query: $searchQuery, type: ISSUE, first: 50, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          ... on Issue {
            number
            title
            body
            url
            state
            issueType {
              name
            }
          }
        }
      }
    }
    """
    search_query = f"repo:{owner}/{repo} type:Epic is:issue"
    cursor = None
    epics = []
    while True:
        variables = {"searchQuery": search_query, "cursor": cursor}
        data = github_graphql(query, variables, token)
        search = data["search"]
        for node in search["nodes"]:
            if node:
                epics.append({
                    "number": node["number"],
                    "title": node["title"],
                    "body": node.get("body", ""),
                    "url": node["url"],
                    "state": node["state"],
                    "issue_type": node.get("issueType", {}).get("name") if node.get("issueType") else None,
                })
        if not search["pageInfo"]["hasNextPage"]:
            break
        cursor = search["pageInfo"]["endCursor"]
    return epics


def fetch_sub_issues(owner, repo, issue_number, token):
    """Fetch sub-issues for a given issue via GraphQL."""
    query = """
    query($owner: String!, $repo: String!, $number: Int!, $cursor: String) {
      repository(owner: $owner, name: $repo) {
        issue(number: $number) {
          subIssues(first: 50, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              number
              title
              body
              url
              state
              issueType {
                name
              }
            }
          }
        }
      }
    }
    """
    cursor = None
    sub_issues = []
    while True:
        variables = {
            "owner": owner,
            "repo": repo,
            "number": issue_number,
            "cursor": cursor,
        }
        data = github_graphql(query, variables, token)
        sub = data["repository"]["issue"]["subIssues"]
        for node in sub["nodes"]:
            if node:
                sub_issues.append({
                    "number": node["number"],
                    "title": node["title"],
                    "body": node.get("body", ""),
                    "url": node["url"],
                    "state": node["state"],
                    "issue_type": node.get("issueType", {}).get("name") if node.get("issueType") else None,
                })
        if not sub["pageInfo"]["hasNextPage"]:
            break
        cursor = sub["pageInfo"]["endCursor"]
    return sub_issues


def detect_jira_link(body):
    """Parse issue body for Jira migration link.

    Looks for patterns like:
      Migrated to Jira: [DRIVER-54](https://scylladb.atlassian.net/browse/DRIVER-54)
      Jira Link: [DRIVER-54](https://scylladb.atlassian.net/browse/DRIVER-54)

    Returns the Jira issue key or None.
    """
    if not body:
        return None
    pattern = r"(?:Migrated to Jira|Jira Link)\s*:\s*\[([A-Z][A-Z0-9]+-\d+)\]"
    match = re.search(pattern, body)
    if match:
        return match.group(1)
    return None


def scan_repo(repo_config, token):
    """Scan a single repo: fetch epics and their sub-issues, detect Jira links."""
    parts = repo_config["github"].split("/")
    owner, repo = parts[0], parts[1]

    print(f"Scanning {owner}/{repo} for epics...", file=sys.stderr)
    epics = fetch_epics(owner, repo, token)
    print(f"  Found {len(epics)} epics", file=sys.stderr)

    results = []
    for epic in epics:
        epic_jira_key = detect_jira_link(epic["body"])
        print(f"  Epic #{epic['number']}: {epic['title']}"
              f"{' -> ' + epic_jira_key if epic_jira_key else ''}", file=sys.stderr)

        # Skip closed epics that were never migrated to Jira
        if epic["state"] == "CLOSED" and not epic_jira_key:
            print(f"    Skipping closed non-migrated epic and its sub-issues", file=sys.stderr)
            continue

        subs = fetch_sub_issues(owner, repo, epic["number"], token)
        sub_results = []
        for sub in subs:
            sub_jira_key = detect_jira_link(sub["body"])
            sub_results.append({
                "issue": sub,
                "jira_key": sub_jira_key,
            })
            if sub_jira_key:
                print(f"    Sub #{sub['number']}: {sub['title']} -> {sub_jira_key}", file=sys.stderr)
            else:
                print(f"    Sub #{sub['number']}: {sub['title']}", file=sys.stderr)

        results.append({
            "epic": epic,
            "jira_key": epic_jira_key,
            "sub_issues": sub_results,
        })

    return results


def find_missing_components(repos, mgr):
    """Find Scylla Components values referenced in config but missing in Jira.

    Returns a list of missing value strings.
    """
    # Collect unique component values from config
    needed = set()
    for repo in repos:
        needed.add(repo["scylla_components"])

    # Fetch existing options from Jira
    field = mgr.find_field("Scylla Components")
    if not field:
        print("Warning: 'Scylla Components' field not found in Jira, "
              "skipping component check", file=sys.stderr)
        return []

    field_id = field["id"]
    contexts = mgr.get_contexts(field_id)
    if not contexts:
        print("Warning: no contexts for 'Scylla Components' field, "
              "skipping component check", file=sys.stderr)
        return []

    context_id = contexts[0]["id"]
    options = mgr.get_options(field_id, context_id)
    existing = {opt["value"] for opt in options}

    missing = sorted(needed - existing)
    if missing:
        print(f"Missing 'Scylla Components' values: {missing}", file=sys.stderr)
    else:
        print("All 'Scylla Components' values exist in Jira", file=sys.stderr)

    return missing


def build_plan(scan_results, repo_config, config, missing_components):
    """Generate a migration plan from scan results."""
    plan = []
    prefix = repo_config["jira_prefix"]
    project = config["jira_project"]
    components = repo_config["scylla_components"]
    type_mapping = config.get("type_mapping", {})
    default_worktype = config.get("default_worktype", "Task")
    component_missing = components in missing_components

    for entry in scan_results:
        epic = entry["epic"]
        epic_jira_key = entry["jira_key"]

        if epic_jira_key:
            plan.append({
                "action": "skip",
                "github_ref": epic["url"],
                "github_title": epic["title"],
                "jira_key": epic_jira_key,
                "reason": "Already migrated",
            })
        else:
            action = "create"
            entry_plan = {
                "action": action,
                "github_ref": epic["url"],
                "github_title": epic["title"],
                "jira_issue_type": "Epic",
                "jira_project": project,
                "scylla_components": components,
                "summary": "{} {}".format(prefix, re.sub(r'^\[.*?\]\s*', '', epic['title'])),
            }
            if component_missing:
                entry_plan["action"] = f"fail: missing Scylla Components option '{components}'"
            plan.append(entry_plan)

        for sub_entry in entry["sub_issues"]:
            sub = sub_entry["issue"]
            sub_jira_key = sub_entry["jira_key"]

            if sub_jira_key:
                plan.append({
                    "action": "skip",
                    "github_ref": sub["url"],
                    "github_title": sub["title"],
                    "jira_key": sub_jira_key,
                    "reason": "Already migrated",
                })
            else:
                issue_type_name = sub.get("issue_type") or ""
                jira_type = type_mapping.get(issue_type_name, default_worktype)

                entry_plan = {
                    "action": "create",
                    "github_ref": sub["url"],
                    "github_title": sub["title"],
                    "jira_issue_type": jira_type,
                    "jira_project": project,
                    "scylla_components": components,
                    "summary": "{} {}".format(prefix, re.sub(r'^\[.*?\]\s*', '', sub['title'])),
                }
                if component_missing:
                    entry_plan["action"] = f"fail: missing Scylla Components option '{components}'"
                if epic_jira_key:
                    entry_plan["jira_parent"] = epic_jira_key
                else:
                    entry_plan["jira_parent_github"] = epic["url"]

                plan.append(entry_plan)

    return plan


def build_field_plan(missing_components):
    """Build plan entries for creating missing Scylla Components options."""
    plan = []
    for value in sorted(missing_components):
        plan.append({
            "action": "create_field_option",
            "field": "Scylla Components",
            "value": value,
        })
    return plan


def report_missing_components(missing_components):
    """Print missing component errors to stderr."""
    print("\nError: the following Scylla Components options do not exist in Jira:",
          file=sys.stderr)
    for value in missing_components:
        print(f"  - action: not allowed to create", file=sys.stderr)
        print(f"    field: Scylla Components", file=sys.stderr)
        print(f"    value: {value}", file=sys.stderr)
    print("\nUse --create-components to create them automatically.", file=sys.stderr)


def execute_plan(plan, config, mgr):
    """Execute migration plan: create field options, Jira issues, and update GitHub issues."""
    from atlassian import Jira

    jira_url = config["jira"]["url"]
    github_token = os.environ.get("GITHUB_TOKEN")

    user_and_key = os.environ.get("USER_AND_KEY_FOR_JIRA_AUTOMATION")
    if not user_and_key or ":" not in user_and_key:
        print("Error: USER_AND_KEY_FOR_JIRA_AUTOMATION env var required (format: user:token)", file=sys.stderr)
        sys.exit(1)
    jira_user, jira_token = user_and_key.split(":", 1)

    jira = Jira(url=jira_url, username=jira_user, password=jira_token)

    # First pass: create field options
    _execute_field_options(plan, mgr)

    # Map github_ref -> created jira key (for linking sub-issues to newly created epics)
    created_keys = {}

    # Second pass: create epics
    for entry in plan:
        if entry["action"] != "create" or entry.get("jira_issue_type") != "Epic":
            continue
        _create_jira_issue(entry, jira, config, github_token, created_keys)

    # Third pass: create sub-issues
    for entry in plan:
        if entry["action"] != "create" or entry.get("jira_issue_type") == "Epic":
            continue

        # Resolve parent key if it references a newly created epic
        if "jira_parent_github" in entry:
            parent_github = entry["jira_parent_github"]
            if parent_github in created_keys:
                entry["jira_parent"] = created_keys[parent_github]
            else:
                print(f"  Warning: parent epic not yet created for {entry['github_ref']}, skipping",
                      file=sys.stderr)
                continue

        _create_jira_issue(entry, jira, config, github_token, created_keys)


def _execute_field_options(plan, mgr):
    """Execute create_field_option plan entries."""
    field_entries = [e for e in plan if e["action"] == "create_field_option"]
    if not field_entries:
        return

    field = mgr.find_field("Scylla Components")
    if not field:
        print("Error: 'Scylla Components' field not found, cannot create options",
              file=sys.stderr)
        sys.exit(1)

    field_id = field["id"]
    contexts = mgr.get_contexts(field_id)
    if not contexts:
        print("Error: no contexts for 'Scylla Components' field", file=sys.stderr)
        sys.exit(1)

    context_id = contexts[0]["id"]
    existing = {opt["value"].lower() for opt in mgr.get_options(field_id, context_id)}

    for entry in field_entries:
        value = entry["value"]
        if value.lower() in existing:
            print(f"  Field option '{value}' already exists, skipping", file=sys.stderr)
            continue
        print(f"  Creating field option: '{value}'", file=sys.stderr)
        result = mgr.add_option(field_id, context_id, value)
        new_options = result.get("options", [])
        if new_options:
            print(f"    Created: '{new_options[0]['value']}' (id={new_options[0]['id']})",
                  file=sys.stderr)
        else:
            print(f"    Created successfully", file=sys.stderr)
        existing.add(value.lower())


def _create_jira_issue(entry, jira, config, github_token, created_keys):
    """Create a single Jira issue and update the corresponding GitHub issue."""
    jira_url = config["jira"]["url"]
    project = entry["jira_project"]
    summary = entry["summary"]
    issue_type = entry["jira_issue_type"]
    components = entry.get("scylla_components", "")
    github_ref = entry["github_ref"]

    fields = {
        "project": {"key": project},
        "summary": summary,
        "issuetype": {"name": issue_type},
        "description": f"Migrated from GitHub: {github_ref}",
    }

    if components:
        fields["components"] = [{"name": components}]

    parent_key = entry.get("jira_parent")
    if parent_key:
        fields["parent"] = {"key": parent_key}

    print(f"  Creating {issue_type} in {project}: {summary}", file=sys.stderr)
    result = jira.issue_create(fields=fields)
    jira_key = result["key"]
    print(f"    Created: {jira_key}", file=sys.stderr)

    created_keys[github_ref] = jira_key

    # Update GitHub issue body with Jira link
    browse_url = f"{jira_url}/browse/{jira_key}"
    _update_github_body(github_ref, jira_key, browse_url, github_token)

    return jira_key


def _update_github_body(github_url, jira_key, browse_url, token):
    """Append 'Migrated to Jira' link to the GitHub issue body."""
    # Parse owner/repo/number from URL
    match = re.match(r"https://github\.com/([^/]+)/([^/]+)/issues/(\d+)", github_url)
    if not match:
        print(f"    Warning: could not parse GitHub URL: {github_url}", file=sys.stderr)
        return

    owner, repo, number = match.group(1), match.group(2), int(match.group(3))

    # Fetch current body via REST API
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/issues/{number}",
        headers=headers,
    )
    resp.raise_for_status()
    current_body = resp.json().get("body", "") or ""

    # Append migration link
    migration_line = f"\n\nMigrated to Jira: [{jira_key}]({browse_url})"
    new_body = current_body + migration_line

    resp = requests.patch(
        f"https://api.github.com/repos/{owner}/{repo}/issues/{number}",
        headers=headers,
        json={"body": new_body},
    )
    resp.raise_for_status()
    print(f"    Updated GitHub issue #{number} with Jira link", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="GitHub-to-Jira Epic Migration Tool"
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--repo",
        help="Scan only a specific repo from config (e.g., scylladb/python-driver)",
    )
    parser.add_argument(
        "--execute-all", action="store_true",
        help="Execute the full migration plan",
    )
    parser.add_argument(
        "--execute",
        help="Execute plan for a specific GitHub issue URL",
    )
    parser.add_argument(
        "--create-components", action="store_true",
        help="Include create_field_option steps in the plan for missing Scylla Components",
    )
    args = parser.parse_args()

    config = load_and_validate_config(args.config)

    # Create JiraManager for field option checks
    user_and_key = os.environ.get("USER_AND_KEY_FOR_JIRA_AUTOMATION")
    if not user_and_key or ":" not in user_and_key:
        print("Error: USER_AND_KEY_FOR_JIRA_AUTOMATION env var required (format: user:token)", file=sys.stderr)
        sys.exit(1)
    jira_user, jira_token = user_and_key.split(":", 1)
    mgr = JiraManager(config["jira"]["url"], jira_user, jira_token)

    # Filter repos if --repo specified
    repos = config["repos"]
    if args.repo:
        repos = [r for r in repos if r["github"] == args.repo]
        if not repos:
            print(f"Error: repo '{args.repo}' not found in config", file=sys.stderr)
            sys.exit(1)

    # Detect missing Scylla Components field values (for all repos, not just filtered)
    print("Checking Scylla Components field values...", file=sys.stderr)
    missing_components = set(find_missing_components(config["repos"], mgr))

    # Build field plan if --create-components; issue entries won't be marked as fail
    if args.create_components and missing_components:
        field_plan = build_field_plan(missing_components)
        issue_missing = set()  # components will be created, so issues are fine
    else:
        field_plan = []
        issue_missing = missing_components  # issues referencing these get action: fail

    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        print("Error: GITHUB_TOKEN env var is required", file=sys.stderr)
        sys.exit(1)

    # Scan and build plan — field_plan first, then issue plan
    full_plan = list(field_plan)
    for repo_config in repos:
        scan_results = scan_repo(repo_config, github_token)
        plan = build_plan(scan_results, repo_config, config, issue_missing)
        full_plan.extend(plan)

    if args.execute_all or args.execute:
        if issue_missing:
            # Print plan with failures to stdout, errors to stderr, then abort
            print(yaml.dump(full_plan, default_flow_style=False, sort_keys=False))
            report_missing_components(sorted(issue_missing))
            sys.exit(1)

        if args.execute:
            filtered = [e for e in full_plan if e.get("github_ref") == args.execute]
            if not filtered:
                print(f"Error: no plan entries found for {args.execute}", file=sys.stderr)
                sys.exit(1)
            # Include field_plan entries when executing a single issue
            execute_plan(field_plan + filtered, config, mgr)
        else:
            execute_plan(full_plan, config, mgr)
    else:
        # Default: print YAML plan to stdout
        print(yaml.dump(full_plan, default_flow_style=False, sort_keys=False))
        if issue_missing:
            report_missing_components(sorted(issue_missing))


if __name__ == "__main__":
    main()
