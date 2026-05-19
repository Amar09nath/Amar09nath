# crew_agents.py
from crewai import Agent
from langchain.llms import Ollama
from vector import vector_store  # import your ChromaDB setup

# Local LLM via Ollama (make sure Ollama is running locally)
llm = Ollama(model="mistral", base_url="http://localhost:11434")

# Retriever Agent: searches ChromaDB for relevant reviews
retriever_agent = Agent(
    role="Retriever",
    goal="Find relevant pizza restaurant reviews",
    backstory="Specialist in searching customer reviews for context.",
    tools=[vector_store.as_retriever()]
)

# Summarizer Agent: generates natural language answers
summarizer_agent = Agent(
    role="Summarizer",
    goal="Summarize reviews into a clear answer",
    backstory="Expert in generating natural language summaries.",
    llm=llm
)