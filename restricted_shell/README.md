# Restricted shell access
To improve security, it is recommended to limit the commands that the user perfoming the backup can execute.

You can specify a command to act as the login shell in **.ssh/authorized_keys** like this:
allowed_backup_commands.py will then verify that all commands are passing the verification.
Note that you need to edit it to list the datasets you want to allow backing up.

```
command=".ssh/allowed_backup_commands.py" ssh-rsa AAAAB3...EjBd user@server
```
