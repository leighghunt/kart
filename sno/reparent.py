import click

import pygit2


@click.command(name="reparent-commit", hidden=True)
@click.pass_context
@click.option(
    "--update-ref",
    help="Which ref to update. Defaults to new_parent, if that's a ref name, or HEAD.",
)
@click.argument(
    "original_commit",
)
@click.argument(
    "new_parent",
)
def reparent_commit(ctx, update_ref, original_commit, new_parent):
    """
    Creates a new commit by copying the tree from an existing commit, onto a base commit.

    With a working copy this is similar to:
        git reset --hard <commit>
        git reset --mixed <new-parent>
        git commit --reuse-message=<commit>

    However this command does not require a working copy, unlike all git helpers to do similar things.

    It is not possible for this command to create a conflict;
    It doesn't apply a changeset; it merely copies a tree into a new commit.
    """
    repo = ctx.obj.repo

    # validation
    original_commit, _ = repo.resolve_refish(original_commit)
    new_parent, new_parent_ref = repo.resolve_refish(new_parent)

    if update_ref is None and new_parent_ref:
        update_ref = new_parent_ref.name
    if update_ref is None and not repo.head_is_detached:
        update_ref = "HEAD"

    tree = original_commit.peel(pygit2.Tree)
    new_commit_id = repo.create_commit(
        update_ref or None,
        original_commit.author,
        repo.committer_signature(),
        original_commit.message,
        tree.id,
        [new_parent.id.hex],
    )
    print(new_commit_id)
