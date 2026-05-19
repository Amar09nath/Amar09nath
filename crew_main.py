# crew_main.py
from crewai import Crew
from crew_agents import retriever_agent, summarizer_agent

def run_query(user_query: str):
    # Define the workflow: first retrieval, then summarization
    crew = Crew(
        agents=[retriever_agent, summarizer_agent],
        tasks=[
            {"agent": retriever_agent, "input": user_query},
            {"agent": summarizer_agent}
        ]
    )
    result = crew.run()
    return result

if __name__ == "__main__":
    query = "Tell me about Joe's Pizza"
    answer = run_query(query)
    print("User Query:", query)
    print("Agent Answer:", answer)