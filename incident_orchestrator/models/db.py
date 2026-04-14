from datetime import datetime, timezone

from sqlalchemy import Index, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Incident(Base):
    __tablename__ = "incidents"

    incident_id: Mapped[str] = mapped_column(Text, primary_key=True)
    claude_session_id: Mapped[str] = mapped_column(Text, nullable=False)
    feishu_root_message_id: Mapped[str | None] = mapped_column(Text)
    feishu_chat_id: Mapped[str | None] = mapped_column(Text)
    service: Mapped[str | None] = mapped_column(Text)
    env: Mapped[str] = mapped_column(Text, default="prod")
    severity: Mapped[str] = mapped_column(Text, default="P2")
    summary: Mapped[str | None] = mapped_column(Text)
    subcode: Mapped[str | None] = mapped_column(Text)
    api_path: Mapped[str | None] = mapped_column(Text)
    trace_id: Mapped[str | None] = mapped_column(Text)
    fingerprint: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="NEW")
    fix_branch: Mapped[str | None] = mapped_column(Text)
    pr_url: Mapped[str | None] = mapped_column(Text)
    analysis_result: Mapped[str | None] = mapped_column(Text)
    worktree_name: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(
        Text, default=lambda: datetime.now(timezone.utc).isoformat()
    )
    updated_at: Mapped[str] = mapped_column(
        Text,
        default=lambda: datetime.now(timezone.utc).isoformat(),
        onupdate=lambda: datetime.now(timezone.utc).isoformat(),
    )

    __table_args__ = (
        Index("idx_incidents_root_msg", "feishu_root_message_id"),
        Index("idx_incidents_session", "claude_session_id"),
        Index("idx_incidents_fingerprint", "fingerprint"),
    )


class IncidentMessage(Base):
    __tablename__ = "incident_messages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)  # user / claude / system
    content: Mapped[str] = mapped_column(Text, nullable=False)
    feishu_message_id: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(
        Text, default=lambda: datetime.now(timezone.utc).isoformat()
    )

    __table_args__ = (Index("idx_messages_incident", "incident_id"),)
