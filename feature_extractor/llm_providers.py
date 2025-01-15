import abc
import dataclasses

from instructor import AsyncInstructor


@dataclasses.dataclass(frozen=True)
class LlmWrapper:
    model: AsyncInstructor
    model_name: str
    api_key: str


class ILlmProvider(abc.ABC):

    @abc.abstractmethod
    def provide_llm(self) -> LlmWrapper:
        raise NotImplementedError

    @abc.abstractmethod
    async def sleep_until_next_ready_async(self):
        raise NotImplementedError
