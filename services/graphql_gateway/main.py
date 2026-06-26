"""
main.py — FastAPI app that mounts the Strawberry GraphQL router

FastAPI handles HTTP + WebSocket at the transport layer.
Strawberry handles the GraphQL protocol on top of that.

Endpoints:
  GET/POST  http://localhost:8000/graphql   → queries and mutations
  WS        ws://localhost:8000/graphql     → subscriptions
  GET       http://localhost:8000/graphql   → opens GraphQL Playground (browser IDE)
"""

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.subscriptions import GRAPHQL_WS_PROTOCOL, GRAPHQL_TRANSPORT_WS_PROTOCOL
from fastapi import FastAPI

from schema import Query, Mutation, Subscription

# Build the schema from our type definitions
schema = strawberry.Schema(
    query        = Query,
    mutation     = Mutation,
    subscription = Subscription,
)

# Mount the GraphQL router on FastAPI
# subscription_protocols: support both WebSocket sub-protocols
graphql_app = GraphQLRouter(
    schema,
    subscription_protocols=[
        GRAPHQL_WS_PROTOCOL,
        GRAPHQL_TRANSPORT_WS_PROTOCOL,
    ]
)

app = FastAPI(title="Real-Time Market Intelligence API")
app.include_router(graphql_app, prefix="/graphql")


@app.get("/")
async def root():
    return {
        "message": "Real-Time Market Intelligence API",
        "graphql":  "http://localhost:8000/graphql",
        "docs":     "http://localhost:8000/docs",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)


