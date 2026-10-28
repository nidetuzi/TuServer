# Agent 项目规则入口

本文件供 Codex 及支持 `AGENTS.md` 的其他 Agent 工具使用，作用域为整个仓库。

## 规则来源

项目规则统一维护在 [`.cursor/rules/`](.cursor/rules/)，该目录是唯一维护源。开始分析、修改、测试或提交前，必须列出并读取其中全部 `.mdc` 文件，不要只读取本文件或根据文件名推测规则。

当前规则入口如下；新增规则后也必须按目录实际内容读取，不以此列表为限：

- [架构约定](.cursor/rules/architecture.mdc)
- [设计中心](.cursor/rules/design.mdc)
- [Go 注释规范](.cursor/rules/go-comments.mdc)
- [Git 中文提交规范](.cursor/rules/git-commit.mdc)

## 适用方式

- `.mdc` 是带 YAML frontmatter 的文本文件；即使工具不原生支持该格式，也必须读取正文并遵守适用规则。
- `alwaysApply: true` 的规则始终适用；带 `globs` 的规则按目标文件范围适用；其他规则按 `description` 和任务相关性判断。
- 不支持规则匹配时，保守地将相关规则作为约束；适用范围不清楚时先确认，不要直接忽略。
- 本入口只负责规则发现，不复制或改写规则正文。新增或调整项目规范应修改 `.cursor/rules/`，避免不同工具维护多份副本。
- 项目规则不能覆盖工具的系统、安全与权限要求；遇到冲突或无法读取规则时，应说明原因，不要声称已经遵守。

架构和模块用法可进一步查阅 [`docs/README.md`](docs/README.md)，但文档不能替代上述规则。
