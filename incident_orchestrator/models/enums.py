from enum import StrEnum


class IncidentStatus(StrEnum):
    NEW = "NEW"
    ANALYZING = "ANALYZING"
    ANALYZED = "ANALYZED"
    FIXING = "FIXING"
    PR_OPEN = "PR_OPEN"
    MERGED = "MERGED"
    REJECTED = "REJECTED"
    ESCALATED = "ESCALATED"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunAction(StrEnum):
    ANALYZE = "analyze"
    FIX = "fix"
    FOLLOWUP = "followup"
