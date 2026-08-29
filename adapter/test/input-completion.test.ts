import assert from "node:assert/strict";
import test from "node:test";

import {
  ChatInputAnswerState,
  ChatInputAnswerValueKind,
  ChatInputQuestionKind,
  type ChatInputMultiSelectQuestion,
  type ChatInputSingleSelectQuestion,
} from "@microsoft/agent-host-protocol";

import { answerQuestion } from "../src/input-completion.js";

const singleSelect: ChatInputSingleSelectQuestion = {
  id: "single",
  kind: ChatInputQuestionKind.SingleSelect,
  message: "Choose or enter a value",
  options: [
    { id: "option-a", label: "选项 A" },
    { id: "option-b", label: "选项 B" },
  ],
  allowFreeformInput: true,
};

test("single-select maps an option label to its stable option ID", () => {
  assert.deepEqual(answerQuestion(singleSelect, "选项 A"), {
    state: ChatInputAnswerState.Submitted,
    value: {
      kind: ChatInputAnswerValueKind.Selected,
      value: "option-a",
    },
  });
});

test("single-select encodes allowed free-form input as text", () => {
  assert.deepEqual(answerQuestion(singleSelect, "今天是星期几？"), {
    state: ChatInputAnswerState.Submitted,
    value: {
      kind: ChatInputAnswerValueKind.Text,
      value: "今天是星期几？",
    },
  });
});

test("single-select rejects free-form input when it is not allowed", () => {
  assert.throws(
    () =>
      answerQuestion(
        { ...singleSelect, allowFreeformInput: false },
        "今天是星期几？",
      ),
    /not an available option/u,
  );
});

test("multi-select preserves selected IDs and allowed free-form values", () => {
  const question: ChatInputMultiSelectQuestion = {
    id: "multi",
    kind: ChatInputQuestionKind.MultiSelect,
    message: "Choose values",
    options: [
      { id: "option-a", label: "选项 A" },
      { id: "option-b", label: "选项 B" },
    ],
    allowFreeformInput: true,
  };

  assert.deepEqual(answerQuestion(question, "选项 A，自定义值"), {
    state: ChatInputAnswerState.Submitted,
    value: {
      kind: ChatInputAnswerValueKind.SelectedMany,
      value: ["option-a"],
      freeformValues: ["自定义值"],
    },
  });
});
