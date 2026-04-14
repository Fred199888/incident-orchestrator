import uvicorn

from incident_orchestrator.app import create_app

app = create_app()

if __name__ == "__main__":
    from incident_orchestrator.config import get_settings

    settings = get_settings()
    uvicorn.run(
        "run:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )
