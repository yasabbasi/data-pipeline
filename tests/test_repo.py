import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_claude_md_is_at_most_40_lines() -> None:
    assert len((ROOT / "CLAUDE.md").read_text().splitlines()) <= 40


def test_workflows_pin_actions_by_commit() -> None:
    for workflow in (ROOT / ".github" / "workflows").glob("*.yml"):
        for used in re.findall(r"uses: (\S+)", workflow.read_text()):
            assert re.fullmatch(r"[\w./-]+@[0-9a-f]{40}", used), used


def test_readme_only_names_make_targets_that_exist() -> None:
    targets = set(re.findall(r"^([a-z-]+):", (ROOT / "Makefile").read_text(), flags=re.M))
    named = set(re.findall(r"`make ([a-z-]+)", (ROOT / "README.md").read_text()))
    assert named <= targets


def test_readme_says_how_contract_v2_came_about() -> None:
    readme = (ROOT / "README.md").read_text()
    how_it_works = readme.split("## How it works")[1].split("\n## ")[0]
    history = how_it_works.split("- **Contract v2 history:**")[1].split("\n- **")[0]
    assert len(history.strip().splitlines()) <= 6
    for link in (
        "https://github.com/yasabbasi/data-pipeline/pull/5",
        "https://github.com/yasabbasi/data-pipeline/pull/7",
        "https://github.com/yasabbasi/task-agent",
        "https://github.com/yasabbasi/mcp-server",
    ):
        assert link in history
