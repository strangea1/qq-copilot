import {
  ChatInputAnswerState,
  ChatInputAnswerValueKind,
  ChatInputQuestionKind,
  ChatInputResponseKind,
  SessionInputRequestKind,
  type ChatInputAnswer,
  type ChatInputQuestion,
} from "@microsoft/agent-host-protocol";

import {
  AhpOperationError,
  type AhpSessionBinding,
} from "./ahp-core.js";

export function buildInputCompletion(
  binding: AhpSessionBinding,
  inputKey: string,
  answer: string,
): {
  readonly requestId: string;
  readonly response: ChatInputResponseKind.Accept;
  readonly answers: Record<string, ChatInputAnswer>;
} {
  const request = binding
    .snapshot()
    .session?.inputNeeded?.find(
      (candidate) =>
        candidate.id === inputKey &&
        candidate.kind === SessionInputRequestKind.ChatInput,
    );
  if (!request || request.kind !== SessionInputRequestKind.ChatInput) {
    throw new AhpOperationError(
      "pending-input-not-found",
      "Input request is no longer pending",
    );
  }
  const questions = request.request.questions ?? [];
  if (questions.length !== 1 || !questions[0]) {
    throw new AhpOperationError(
      "ambiguous-input",
      "QQ can answer only a single-field input request",
    );
  }
  return {
    requestId: request.request.id,
    response: ChatInputResponseKind.Accept,
    answers: {
      [questions[0].id]: answerQuestion(questions[0], answer),
    },
  };
}

export function answerQuestion(
  question: ChatInputQuestion,
  answer: string,
): ChatInputAnswer {
  const submitted = ChatInputAnswerState.Submitted;
  switch (question.kind) {
    case ChatInputQuestionKind.Text:
      return {
        state: submitted,
        value: { kind: ChatInputAnswerValueKind.Text, value: answer },
      };
    case ChatInputQuestionKind.Number:
    case ChatInputQuestionKind.Integer: {
      const value = Number(answer);
      if (
        !Number.isFinite(value) ||
        (question.kind === ChatInputQuestionKind.Integer &&
          !Number.isInteger(value))
      ) {
        throw new AhpOperationError("invalid-command", "Answer is not numeric");
      }
      return {
        state: submitted,
        value: { kind: ChatInputAnswerValueKind.Number, value },
      };
    }
    case ChatInputQuestionKind.Boolean: {
      const normalized = answer.trim().toLocaleLowerCase("zh-CN");
      if (!["是", "否", "true", "false", "yes", "no"].includes(normalized)) {
        throw new AhpOperationError("invalid-command", "Answer is not boolean");
      }
      return {
        state: submitted,
        value: {
          kind: ChatInputAnswerValueKind.Boolean,
          value: ["是", "true", "yes"].includes(normalized),
        },
      };
    }
    case ChatInputQuestionKind.SingleSelect: {
      const option = findOption(question, answer);
      if (option) {
        return {
          state: submitted,
          value: {
            kind: ChatInputAnswerValueKind.Selected,
            value: option.id,
          },
        };
      }
      if (!question.allowFreeformInput) {
        throw new AhpOperationError(
          "invalid-command",
          "Answer is not an available option",
        );
      }
      return {
        state: submitted,
        value: { kind: ChatInputAnswerValueKind.Text, value: answer },
      };
    }
    case ChatInputQuestionKind.MultiSelect: {
      const selected: string[] = [];
      const freeformValues: string[] = [];
      for (const value of answer
        .split(/[,，]/u)
        .map((item) => item.trim())
        .filter(Boolean)) {
        const option = findOption(question, value);
        if (option) {
          selected.push(option.id);
        } else if (question.allowFreeformInput) {
          freeformValues.push(value);
        } else {
          throw new AhpOperationError(
            "invalid-command",
            "Answer is not an available option",
          );
        }
      }
      if (selected.length === 0 && freeformValues.length === 0) {
        throw new AhpOperationError(
          "invalid-command",
          "At least one option or free-form value is required",
        );
      }
      return {
        state: submitted,
        value: {
          kind: ChatInputAnswerValueKind.SelectedMany,
          value: selected,
          ...(freeformValues.length > 0 ? { freeformValues } : {}),
        },
      };
    }
  }
}

function findOption(
  question: Extract<
    ChatInputQuestion,
    {
      kind:
        | ChatInputQuestionKind.SingleSelect
        | ChatInputQuestionKind.MultiSelect;
    }
  >,
  answer: string,
): { readonly id: string } | undefined {
  return question.options.find(
    (candidate) => candidate.id === answer || candidate.label === answer,
  );
}
