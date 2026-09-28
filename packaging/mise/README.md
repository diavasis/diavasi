# mise

`diavasi` is not in the built-in mise registry. Use the GitHub backend:

```bash
mise use -g github:diavasis/diavasi@0.13.0
```

Or in `mise.toml`:

```toml
[tools]
"github:diavasis/diavasi" = "0.13.0"
```

To keep a short name in `.tool-versions` (`diavasi 0.13.0`):

```toml
[tool_alias]
diavasi = "github:diavasis/diavasi"
```

If multiple assets match, narrow with `matching = "diavasi-"`.
