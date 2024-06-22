import enum
import re
from dataclasses import dataclass, field
from typing import Sequence

import langchain_text_splitters
import pandas as pd
import pydantic
import sentence_transformers
from langchain_core.language_models import BaseLanguageModel
from loguru import logger

from bio_data_harmoniser.core import llms


class RagAlgorithm(enum.Enum):
    NON_AGENTIC = "non_agentic"
    AGENTIC = "agentic"


def format_context(context: list[str]) -> str:
    return "\n".join([f"<ctx index={i}>\n{text}\n</ctx>" for i, text in enumerate(context)])


def context_answers_question(context: list[str], question: str, llm: BaseLanguageModel) -> bool:
    prompt = """
    Given the following context and question, can the question be answered by using the context?
    
    Context:
    
    <context>
    {context}
    </context>
    
    Question:
    
    <question>
    {question}
    </question>
    
    Simply answer by saying "Yes" or "No". Do not explain your answer.
    """

    response = llm.predict(
        llms.clean_prompt_formatting(prompt).format(
            context=format_context(context),
            question=question,
        )
    )
    logger.info(f"Is the context sufficient for answering the question: {response}")
    return "yes" in re.sub(r"\W", "", response.lower())


def update_question_to_get_better_context(context: list[str], question: str, llm: BaseLanguageModel) -> str:
    prompt = """
    Given the following context and question, how could the question be improved to get the relevant context to answer it?
    
    Context:
    
    <context>
    {context}
    </context>
    
    Question:
    
    <question>
    {question}
    </question>
    
    Please provide an improved question to improve the context returned in the search results. Provide only the question, and nothing else.
    """

    response = llm.predict(
        llms.clean_prompt_formatting(prompt).format(
            context=format_context(context),
            question=question,
        )
    )
    return response


def get_context(dense_retriever: llms.DenseRetriever, question: str) -> list[str]:
    results = dense_retriever.retrieve([question], top_k=10)[0]
    return results["text"].tolist()


class Reference(pydantic.BaseModel):
    text: str
    url: str | None = None


class Response(pydantic.BaseModel):
    answer: str
    references: list[Reference] = pydantic.Field(default_factory=list)


@dataclass
class RetrievalAugmentedGenerator:
    dense_retriever: llms.DenseRetriever
    llm: BaseLanguageModel = field(default_factory=llms.get_llm)
    algorithm: RagAlgorithm = RagAlgorithm.NON_AGENTIC

    def query(self, query: str) -> Response:
        # TODO: add explanations to the response
        prompt = """
        Given the following context, what is the answer to the question?

        Context:

        <context>
        {context}
        </context>

        Question:

        <question>
        {question}
        </question>

        Provide a short answer to the question with citations to sources used in the context. The citations should be given as a list of integers, where each integer represents the index of the chunk in the context that the citation is for.
        For example, if you used `<ctx index=0>` to answer the question, the citation should be `[[0]]`. If you used `<ctx index=1>` and `<ctx index=2>` to answer the question, the citation should be `[[1,2]]`, and so on. The citations should be given after the answer, and nothing should follow the citations.
        Do not explain your answer.
        """

        def _answer_to_response(answer: str, ctx: list[str]) -> Response:
            def _to_int(match: str) -> int | None:
                try:
                    return int(match)
                except ValueError:
                    return None

            def _clean_answer(a: str) -> str:
                a = re.sub(r"\[\[[\d,\s]+\]\]", "", a, flags=re.MULTILINE)
                return a.strip().strip(".")

            citations = [
                _to_int(match) for match in re.findall(r"\[\[(\d+)]", answer, re.MULTILINE)
            ]
            citations = [citation for citation in citations if citation is not None and citation < len(ctx)]
            return Response(answer=_clean_answer(answer), references=[Reference(text=ctx[citation]) for citation in citations])

        logger.info(f"Running retrieval augmented generation for query: {query!r}")

        texts = get_context(self.dense_retriever, question=query)
        if self.algorithm == RagAlgorithm.AGENTIC:
            n_tries = 4
            context = texts
            question = query
            while n_tries > 0:
                if context_answers_question(context, question, self.llm):
                    logger.info(
                        "The context retrieved is deemed sufficient for answering the question. "
                        f"Question: {question!r}."
                    )
                    break
                logger.info(
                    "The context retrieved is insufficient for answering the question. "
                    f"Question: {question!r}."
                )
                question = update_question_to_get_better_context(
                    context=context,
                    question=question,
                    llm=self.llm,
                )
                logger.info(f"Updated question: {question!r}.")
                context = get_context(self.dense_retriever, question=question)
                n_tries -= 1

        llm_answer = self.llm.predict(
            llms.clean_prompt_formatting(prompt).format(
                context=format_context(texts),
                question=query,
            )
        )
        return _answer_to_response(llm_answer, texts)

    @classmethod
    def from_texts(
        cls,
        texts: Sequence[str],
        llm: BaseLanguageModel | None = None,
        encoder: sentence_transformers.SentenceTransformer | None = None,
        algorithm: RagAlgorithm = RagAlgorithm.NON_AGENTIC,
        chunk_size: int = 1000,
    ) -> "RetrievalAugmentedGenerator":
        llm = llm or llms.get_llm()
        encoder = encoder or llms.get_encoder()
        splitter = langchain_text_splitters.CharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=0,
        )

        all_sentences: list[str] = []
        for text in texts:
            sentences = splitter.split_text(
                "\n".join([line.strip() for line in text.splitlines(keepends=False)])
            )
            all_sentences.extend(sentences)
        logger.info(f"Split {len(texts)} texts into {len(all_sentences)} sentences.")
        logger.info(f"Encoding {len(all_sentences)} sentences.")
        embeddings = encoder.encode(
            all_sentences,
            convert_to_numpy=True,
            show_progress_bar=True,
        )
        logger.info(f"{len(embeddings)} sentences encoded.")
        return cls(
            dense_retriever=llms.DenseRetriever(
                index=llms.Index(
                    vectors=embeddings,
                    metadata=pd.DataFrame(
                        {
                            "text": all_sentences,
                        }
                    ),
                ),
                encoder=encoder,
            ),
            llm=llm,
            algorithm=algorithm,
        )
