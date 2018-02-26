# git

A generic Git backend source that can be used with services such a GitHub and BitBucket.

`ssh://user@host/owner/repo/path[?ssh-key-path=/home/user/key]`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| ssh-key-path | | the path to an ssh private key on your local disk |
| user | | the user to authenticate with when using ssh |
| owner | | the repo owner |
| repo | | the name of the repository |
| path | Config.Path | path in repo to migrations |

\* If no `ssh-key-path` parameter is passed then migrate will attempt to use `id_rsa` or `id_dsa` stored in your homes `.ssh` folder.

`https://[user][:password]@host/owner/repo/path`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| user | | the user to authenticate with when using ssh |
| password | | the password to use when authenticating as user |
| owner | | the repo owner |
| repo | | the name of the repository |
| path | Config.Path | path in repo to migrations |
