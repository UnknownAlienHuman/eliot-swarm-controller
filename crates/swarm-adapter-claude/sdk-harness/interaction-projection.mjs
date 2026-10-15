function record(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function boundedText(value, maximum) {
  return typeof value === 'string' && value.length > 0 && value.length <= maximum
    && !/[\u0000-\u001f\u007f]/.test(value);
}

export function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (record(value)) {
    const keys = Object.keys(value).sort();
    return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

export function redactedText(value, secrets = [], maximum = 4096) {
  if (typeof value !== 'string') return null;
  let text = value;
  for (const secret of secrets) {
    if (typeof secret === 'string' && secret.length > 0) {
      text = text.split(secret).join('[redacted]');
    }
  }
  return text
    .replace(/\b(?:Bearer|Basic)\s+[^\s,;]+/gi, '[redacted credential]')
    .replace(/\b(?:sk-ant-|gh[pousr]_|github_pat_|AKIA)[A-Za-z0-9_-]{12,}\b/g, '[redacted token]')
    .replace(/([?&](?:access_token|api[_-]?key|password|secret|token)=)[^&#\s]*/gi, '$1[redacted]')
    .replace(/\b[A-Za-z]:\\(?:[^\\\s]+\\)*[^\\\s]*/g, '[redacted path]')
    .replace(/\/(?:Users|home)\/[^\s"'<>]+/g, '[redacted path]')
    .slice(0, maximum);
}

function permissionProjection(input) {
  if (!record(input)) throw new Error('INTERACTION_PROJECTION_INVALID');
  const entries = Object.values(input);
  return {
    field_count: entries.length,
    fields: entries.slice(0, 64).map((value, index) => ({
      name: `field_${index + 1}`,
      type: Array.isArray(value) ? 'array'
        : value === null ? 'null'
          : typeof value === 'object' ? 'object' : typeof value,
    })),
  };
}

function questionProjection(input, secrets) {
  if (!record(input) || !Array.isArray(input.questions)
      || input.questions.length < 1 || input.questions.length > 16
      || new Set(input.questions.map(question => question?.question)).size !== input.questions.length) {
    throw new Error('INTERACTION_PROJECTION_INVALID');
  }
  const questions = input.questions.map(question => {
    if (!record(question) || !boundedText(question.question, 4096)
        || !boundedText(question.header, 12) || typeof question.multiSelect !== 'boolean'
        || !Array.isArray(question.options) || question.options.length < 2
        || question.options.length > 4
        || !question.options.every(option => record(option)
          && boundedText(option.label, 256) && boundedText(option.description, 2048)
          && (option.preview === undefined || (typeof option.preview === 'string' && option.preview.length <= 4096)))) {
      throw new Error('INTERACTION_PROJECTION_INVALID');
    }
    return {
      question: redactedText(question.question, secrets, 4096),
      header: redactedText(question.header, secrets, 64),
      multiSelect: question.multiSelect,
      options: question.options.map(option => {
        if (!record(option)) throw new Error('INTERACTION_PROJECTION_INVALID');
        return {
          label: redactedText(option.label, secrets, 256),
          description: redactedText(option.description, secrets, 2048),
        };
      }),
    };
  });
  if (questions.some(question => question.question === null || question.header === null
      || typeof question.multiSelect !== 'boolean'
      || question.options.some(option => option.label === null || option.description === null))
      || new Set(questions.map(question => question.question)).size !== questions.length) {
    throw new Error('INTERACTION_PROJECTION_INVALID');
  }
  return { questions };
}

export function projectInteractionInput(kind, input, secrets = []) {
  if (kind === 'permission') return permissionProjection(input);
  if (kind === 'question') return questionProjection(input, secrets);
  throw new Error('INTERACTION_PROJECTION_INVALID');
}

export function restoreQuestionReply(rawInput, projectedInput, reply) {
  const rawQuestions = rawInput?.questions;
  const visibleQuestions = projectedInput?.questions;
  const updated = reply?.updated_input;
  if (!Array.isArray(rawQuestions) || !Array.isArray(visibleQuestions)
      || rawQuestions.length === 0 || rawQuestions.length !== visibleQuestions.length
      || !record(updated) || !Array.isArray(updated.questions)
      || canonicalJson(updated.questions) !== canonicalJson(visibleQuestions)
      || !record(updated.answers) || Object.keys(updated.answers).length !== visibleQuestions.length) {
    throw new Error('INTERACTION_REPLY_MISMATCH');
  }
  const answers = {};
  const seenRaw = new Set();
  const seenVisible = new Set();
  for (let index = 0; index < visibleQuestions.length; index++) {
    const raw = rawQuestions[index]?.question;
    const visible = visibleQuestions[index]?.question;
    if (typeof raw !== 'string' || typeof visible !== 'string'
        || seenRaw.has(raw) || seenVisible.has(visible)
        || !Object.hasOwn(updated.answers, visible)) {
      throw new Error('INTERACTION_REPLY_MISMATCH');
    }
    seenRaw.add(raw);
    seenVisible.add(visible);
    answers[raw] = updated.answers[visible];
  }
  return { questions: rawQuestions, answers };
}
