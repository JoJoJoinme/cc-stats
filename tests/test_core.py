from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from cc_stats.collectors.claude_code import parse_claude_transcript
from cc_stats.collectors.costrict import parse_costrict_task
from cc_stats.server.app import TEMPLATES
from cc_stats.server.db import capability_stats, connect_db, daily_stats, get_session_detail, list_sessions, monthly_stats, upsert_session, weekly_stats
from starlette.requests import Request


class ClaudeCollectorTest(unittest.TestCase):
    def test_parse_claude_transcript(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transcript = Path(tmpdir) / "abc.jsonl"
            rows = [
                {
                    "type": "user",
                    "timestamp": "2026-04-17T01:00:00+00:00",
                    "sessionId": "abc",
                    "cwd": "/tmp/project",
                    "uuid": "u1",
                    "message": {"role": "user", "content": "please fix failing test"},
                },
                {
                    "type": "assistant",
                    "timestamp": "2026-04-17T01:00:05+00:00",
                    "uuid": "a1",
                    "message": {
                        "id": "m1",
                        "model": "claude-sonnet-4-6",
                        "role": "assistant",
                        "content": [
                            {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "pytest -q"}}
                        ],
                        "usage": {"input_tokens": 10, "output_tokens": 20},
                    },
                },
                {
                    "type": "user",
                    "timestamp": "2026-04-17T01:00:06+00:00",
                    "uuid": "u2",
                    "message": {
                        "role": "user",
                        "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "1 failed", "is_error": False}],
                    },
                },
                {
                    "type": "assistant",
                    "timestamp": "2026-04-17T01:00:09+00:00",
                    "uuid": "a2",
                    "message": {
                        "id": "m2",
                        "model": "claude-sonnet-4-6",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "I found the failing assertion and updated the parser."}],
                        "usage": {"input_tokens": 5, "output_tokens": 30},
                    },
                },
            ]
            transcript.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            session = parse_claude_transcript(transcript)
            self.assertEqual(session.session_id, "claude-code:abc")
            self.assertEqual(session.turn_count, 1)
            self.assertEqual(session.tool_call_count, 1)
            self.assertEqual(session.total_input_tokens, 15)
            self.assertEqual(session.total_output_tokens, 50)
            self.assertEqual(session.tool_calls[0].success, True)
            self.assertEqual(session.category, "debug")

    def test_parse_claude_skill_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transcript = Path(tmpdir) / "skill.jsonl"
            rows = [
                {
                    "type": "user",
                    "timestamp": "2026-04-18T03:37:09.895Z",
                    "sessionId": "skill",
                    "cwd": "/tmp/project",
                    "uuid": "u1",
                    "message": {
                        "role": "user",
                        "content": "<command-message>clash-gist-sync-cskill</command-message>\n<command-name>/clash-gist-sync-cskill</command-name>",
                    },
                },
                {
                    "type": "user",
                    "timestamp": "2026-04-18T03:37:09.895Z",
                    "isMeta": True,
                    "cwd": "/tmp/project",
                    "sessionId": "skill",
                    "uuid": "u2",
                    "message": {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Base directory for this skill: /root/.claude/skills/clash-gist-sync-cskill\n\n# skill body",
                            }
                        ],
                    },
                },
                {
                    "type": "assistant",
                    "timestamp": "2026-04-18T03:37:25.931Z",
                    "uuid": "a1",
                    "message": {
                        "id": "m1",
                        "model": "claude-sonnet-4-6",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "Please send the YAML file."}],
                        "usage": {"input_tokens": 12, "output_tokens": 34},
                    },
                },
            ]
            transcript.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            session = parse_claude_transcript(transcript)
            self.assertTrue(session.has_skill)
            self.assertEqual(session.tool_call_count, 1)
            self.assertEqual(session.tool_calls[0].tool_kind, "skill")
            self.assertEqual(session.tool_calls[0].skill_name, "clash-gist-sync-cskill")
            self.assertEqual(session.user_turn_count, 1)

    def test_parse_claude_invoked_skills_attachment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transcript = Path(tmpdir) / "invoked.jsonl"
            rows = [
                {
                    "type": "system",
                    "timestamp": "2026-04-18T03:36:33.861Z",
                    "sessionId": "invoked",
                    "cwd": "/tmp/project",
                    "uuid": "s1",
                },
                {
                    "type": "attachment",
                    "timestamp": "2026-04-18T03:36:40.000Z",
                    "sessionId": "invoked",
                    "cwd": "/tmp/project",
                    "uuid": "at1",
                    "attachment": {
                        "type": "invoked_skills",
                        "skills": [
                            {
                                "name": "commit",
                                "path": "/root/.claude/skills/commit/SKILL.md",
                                "content": "# skill body",
                            }
                        ],
                    },
                },
                {
                    "type": "assistant",
                    "timestamp": "2026-04-18T03:36:45.000Z",
                    "uuid": "a1",
                    "message": {
                        "id": "m1",
                        "model": "claude-sonnet-4-6",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "I am ready to help with the commit."}],
                        "usage": {"input_tokens": 10, "output_tokens": 20},
                    },
                },
            ]
            transcript.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            session = parse_claude_transcript(transcript)
            self.assertTrue(session.has_skill)
            self.assertEqual(session.tool_call_count, 1)
            self.assertEqual(session.tool_calls[0].skill_name, "commit")
            self.assertIn("commit", session.summary.lower())

    def test_parse_claude_mcp_availability_attachment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transcript = Path(tmpdir) / "mcp-available.jsonl"
            rows = [
                {
                    "type": "system",
                    "timestamp": "2026-04-18T03:36:33.861Z",
                    "sessionId": "mcp-available",
                    "cwd": "/tmp/project",
                    "uuid": "s1",
                },
                {
                    "type": "attachment",
                    "timestamp": "2026-04-18T03:36:40.000Z",
                    "sessionId": "mcp-available",
                    "cwd": "/tmp/project",
                    "uuid": "at1",
                    "attachment": {
                        "type": "deferred_tools_delta",
                        "addedNames": ["mcp__gemini__gemini", "ReadMcpResourceTool"],
                        "addedLines": ["mcp__gemini__gemini", "ReadMcpResourceTool"],
                        "removedNames": [],
                    },
                },
                {
                    "type": "assistant",
                    "timestamp": "2026-04-18T03:36:45.000Z",
                    "uuid": "a1",
                    "message": {
                        "id": "m1",
                        "model": "claude-sonnet-4-6",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "MCP tools are ready."}],
                        "usage": {"input_tokens": 10, "output_tokens": 20},
                    },
                },
            ]
            transcript.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            session = parse_claude_transcript(transcript)
            self.assertFalse(session.has_mcp)
            self.assertEqual(session.extra_json["available_mcp_servers"], ["gemini"])
            self.assertEqual(session.extra_json["available_mcp_tools"], ["mcp__gemini__gemini"])


class CostrictCollectorTest(unittest.TestCase):
    def test_parse_costrict_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage_root = Path(tmpdir) / "zgsm-ai.zgsm"
            task_dir = storage_root / "tasks" / "task-1"
            task_dir.mkdir(parents=True)
            (task_dir / "history_item.json").write_text(
                json.dumps(
                    {
                        "id": "task-1",
                        "task": "Add session summary API",
                        "ts": 1713315600000,
                        "tokensIn": 120,
                        "tokensOut": 300,
                        "cacheWrites": 20,
                        "cacheReads": 15,
                        "totalCost": 0.42,
                        "workspace": "/tmp/demo",
                        "status": "completed",
                    }
                ),
                encoding="utf-8",
            )
            (task_dir / "task_metadata.json").write_text(json.dumps({"task": "Add session summary API"}), encoding="utf-8")
            (task_dir / "api_conversation_history.json").write_text(
                json.dumps(
                    [
                        {"role": "user", "ts": 1713315500000, "content": [{"type": "text", "text": "Add a summary API"}]},
                        {
                            "role": "assistant",
                            "ts": 1713315550000,
                            "content": [{"type": "tool_use", "id": "tool-1", "name": "write_to_file", "input": {"path": "api.py"}}],
                            "model": "gpt-4.1",
                        },
                        {
                            "role": "assistant",
                            "ts": 1713315600000,
                            "content": [{"type": "text", "text": "Added the API and wired it into the router."}],
                            "model": "gpt-4.1",
                        },
                    ]
                ),
                encoding="utf-8",
            )
            (task_dir / "ui_messages.json").write_text("[]", encoding="utf-8")

            session = parse_costrict_task(task_dir, storage_root)
            self.assertEqual(session.session_id, "costrict-ide:task-1")
            self.assertEqual(session.status, "completed")
            self.assertEqual(session.total_cost, 0.42)
            self.assertEqual(session.turn_count, 1)
            self.assertEqual(session.tool_call_count, 1)
            self.assertEqual(session.category, "new_feature")


class DbTest(unittest.TestCase):
    def test_upsert_and_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "db.sqlite3"
            conn = connect_db(str(db_path))
            with tempfile.TemporaryDirectory() as transcript_dir:
                transcript = Path(transcript_dir) / "abc.jsonl"
                transcript.write_text(
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "type": "user",
                                    "timestamp": "2026-04-17T01:00:00+00:00",
                                    "sessionId": "abc",
                                    "cwd": "/tmp/project",
                                    "uuid": "u1",
                                    "message": {"role": "user", "content": "write docs for deploy"},
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "assistant",
                                    "timestamp": "2026-04-17T01:00:02+00:00",
                                    "uuid": "a1",
                                    "message": {
                                        "id": "m1",
                                        "model": "claude-sonnet-4-6",
                                        "role": "assistant",
                                        "content": [{"type": "text", "text": "I drafted the deployment guide."}],
                                        "usage": {"input_tokens": 4, "output_tokens": 18},
                                    },
                                }
                            ),
                        ]
                    ),
                    encoding="utf-8",
                )
                session = parse_claude_transcript(transcript)
                upsert_session(conn, session)

            result = list_sessions(conn, {"category": "docs", "limit": 10, "offset": 0})
            self.assertEqual(result["total"], 1)
            detail = get_session_detail(conn, session.session_id)
            self.assertIsNotNone(detail)
            self.assertEqual(detail["turn_count"], 1)
            self.assertEqual(daily_stats(conn, limit=10)[0]["day"], "2026-04-17")
            self.assertTrue(weekly_stats(conn, limit=10)[0]["week"].startswith("2026-W"))
            self.assertEqual(monthly_stats(conn, limit=10)[0]["month"], "2026-04")
            conn.close()

    def test_capability_breakdown_distinguishes_actual_and_topic(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "db.sqlite3"
            conn = connect_db(str(db_path))

            with tempfile.TemporaryDirectory() as transcript_dir:
                skill_transcript = Path(transcript_dir) / "skill.jsonl"
                skill_transcript.write_text(
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "type": "user",
                                    "timestamp": "2026-04-18T03:37:09.895Z",
                                    "sessionId": "skill",
                                    "cwd": "/tmp/project",
                                    "uuid": "u1",
                                    "message": {
                                        "role": "user",
                                        "content": "<command-message>commit</command-message>\n<command-name>/commit</command-name>",
                                    },
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "user",
                                    "timestamp": "2026-04-18T03:37:09.895Z",
                                    "isMeta": True,
                                    "cwd": "/tmp/project",
                                    "sessionId": "skill",
                                    "uuid": "u2",
                                    "message": {
                                        "role": "user",
                                        "content": [{"type": "text", "text": "Base directory for this skill: /root/.claude/skills/commit\n\n# skill body"}],
                                    },
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "assistant",
                                    "timestamp": "2026-04-18T03:37:25.931Z",
                                    "uuid": "a1",
                                    "message": {
                                        "id": "m1",
                                        "model": "claude-sonnet-4-6",
                                        "role": "assistant",
                                        "content": [{"type": "text", "text": "Ready to help commit changes."}],
                                        "usage": {"input_tokens": 12, "output_tokens": 34},
                                    },
                                }
                            ),
                        ]
                    ),
                    encoding="utf-8",
                )
                upsert_session(conn, parse_claude_transcript(skill_transcript))

            with tempfile.TemporaryDirectory() as task_tmpdir:
                storage_root = Path(task_tmpdir) / "zgsm-ai.zgsm"
                task_dir = storage_root / "tasks" / "task-mcp"
                task_dir.mkdir(parents=True)
                (task_dir / "history_item.json").write_text(
                    json.dumps(
                        {
                            "id": "task-mcp",
                            "task": "Install an MCP server",
                            "ts": 1713315600000,
                            "workspace": "/tmp/demo",
                            "status": "completed",
                        }
                    ),
                    encoding="utf-8",
                )
                (task_dir / "task_metadata.json").write_text(json.dumps({"task": "Install an MCP server"}), encoding="utf-8")
                (task_dir / "api_conversation_history.json").write_text(
                    json.dumps(
                        [
                            {"role": "user", "ts": 1713315500000, "content": [{"type": "text", "text": "Set up an MCP server"}]},
                            {
                                "role": "assistant",
                                "ts": 1713315550000,
                                "content": [{"type": "tool_use", "id": "tool-1", "name": "load_mcp_documentation", "input": {}}],
                                "model": "gpt-4.1",
                            },
                        ]
                    ),
                    encoding="utf-8",
                )
                (task_dir / "ui_messages.json").write_text("[]", encoding="utf-8")
                upsert_session(conn, parse_costrict_task(task_dir, storage_root))

            breakdown = capability_stats(conn)
            self.assertEqual(breakdown["skills"]["actual_sessions"], 1)
            self.assertEqual(breakdown["skills"]["call_count"], 1)
            self.assertEqual(breakdown["mcp"]["actual_sessions"], 0)
            self.assertEqual(breakdown["mcp"]["available_sessions"], 0)
            self.assertEqual(breakdown["mcp"]["topic_sessions"], 1)

            with tempfile.TemporaryDirectory() as transcript_dir:
                transcript = Path(transcript_dir) / "available.jsonl"
                transcript.write_text(
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "type": "system",
                                    "timestamp": "2026-04-18T03:36:33.861Z",
                                    "sessionId": "available",
                                    "cwd": "/tmp/project",
                                    "uuid": "s1",
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "attachment",
                                    "timestamp": "2026-04-18T03:36:40.000Z",
                                    "sessionId": "available",
                                    "cwd": "/tmp/project",
                                    "uuid": "at1",
                                    "attachment": {
                                        "type": "deferred_tools_delta",
                                        "addedNames": ["mcp__gemini__gemini"],
                                        "addedLines": ["mcp__gemini__gemini"],
                                        "removedNames": [],
                                    },
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "assistant",
                                    "timestamp": "2026-04-18T03:36:45.000Z",
                                    "uuid": "a1",
                                    "message": {
                                        "id": "m1",
                                        "model": "claude-sonnet-4-6",
                                        "role": "assistant",
                                        "content": [{"type": "text", "text": "The MCP tool catalog is available."}],
                                        "usage": {"input_tokens": 10, "output_tokens": 20},
                                    },
                                }
                            ),
                        ]
                    ),
                    encoding="utf-8",
                )
                upsert_session(conn, parse_claude_transcript(transcript))

            breakdown = capability_stats(conn)
            self.assertEqual(breakdown["mcp"]["available_sessions"], 1)
            claude_source = next(item for item in breakdown["by_source"] if item["source"] == "claude-code")
            self.assertEqual(claude_source["mcp_available_sessions"], 1)
            conn.close()


class HtmlRoutesTest(unittest.TestCase):
    def test_sessions_page_renders_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "db.sqlite3"
            conn = connect_db(str(db_path))
            with tempfile.TemporaryDirectory() as transcript_dir:
                transcript = Path(transcript_dir) / "abc.jsonl"
                transcript.write_text(
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "type": "user",
                                    "timestamp": "2026-04-17T01:00:00+00:00",
                                    "sessionId": "abc",
                                    "cwd": "/tmp/project",
                                    "uuid": "u1",
                                    "message": {"role": "user", "content": "show recent sessions"},
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "assistant",
                                    "timestamp": "2026-04-17T01:00:02+00:00",
                                    "uuid": "a1",
                                    "message": {
                                        "id": "m1",
                                        "model": "claude-sonnet-4-6",
                                        "role": "assistant",
                                        "content": [{"type": "text", "text": "I listed the recent sessions."}],
                                        "usage": {"input_tokens": 4, "output_tokens": 18},
                                    },
                                }
                            ),
                        ]
                    ),
                    encoding="utf-8",
                )
                upsert_session(conn, parse_claude_transcript(transcript))
            result = list_sessions(conn, {"limit": 10, "offset": 0})
            conn.close()

            request = Request(
                {
                    "type": "http",
                    "method": "GET",
                    "path": "/sessions",
                    "headers": [],
                    "query_string": b"",
                    "scheme": "http",
                    "server": ("testserver", 80),
                    "client": ("testclient", 123),
                }
            )
            response = TEMPLATES.TemplateResponse(
                request,
                "sessions.html",
                {"request": request, "result": result, "filters": {"source": "", "user_id": "", "repo": "", "category": "", "text_query": "", "sort": ""}},
            )
            self.assertEqual(response.status_code, 200)
            self.assertIn("claude-code", response.body.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
