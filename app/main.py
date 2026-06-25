from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.database  import engine, Base
from app.messaging  import connect_rabbitmq, disconnect_rabbitmq
from app.routers   import leads, events


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Base.metadata.create_all(bind=engine)
    print("✓ Database tables ready")
    connect_rabbitmq()
    yield
    # Shutdown
    disconnect_rabbitmq()


app = FastAPI(
    title="Sales CRM API",
    description="FastAPI backend for the Oracle APEX Sales Pipeline CRM",
    version="2.0.0",
    lifespan=lifespan
)

app.include_router(leads.router)
app.include_router(events.router)


@app.get("/")
def root():
    return {"message": "Sales CRM API is running"}

@app.get("/health")
def health():
    return {"status": "ok"}