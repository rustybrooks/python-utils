# publish

This is a small library intended to help with publishing python packages.
It's intended to be run in github actions although it has some use run locally in dry-run mode.

## Usage

If run from the root of a project, it expects a path to the directory that contains the package
you want to publish. For simplicity I would recommend having your publishable packages all at the
same level in the file system.

`publish ./path-to-package` with no other args will:

* determine the current version of the package and calculate the next version
* write the new version to pyproject.toml
* it will configure poetry to install from and publish to a package index
* it will build and publish the package to a package index
* commit pyproject back to github

This means that if run in `main` as an action on merge-to-main, it will automatically increase the
version, publish the package, and update pyproject.toml to the new version. Developers will not need
to manually update versions or manually publish packages.

Some addtional options
`--dry-run` does a dry run, showing the current/new version, and showing the commands that *would*
be run if not in dry run mode
`--alpha` publishes an alpha version. This could be used in PRs for example. 
