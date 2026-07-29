def numeric_ids:
  [
    ..
    | objects
    | (
        .id?,
        .items?[]?,
        .impls?[]?,
        .fields?[]?,
        .variants?[]?,
        .implementations?[]?
      )
    | select(type == "number")
  ]
  | unique;

. as $document
| def item_refs($id):
    ($document.index[($id | tostring)] // {} | numeric_ids);
  def local_item($id):
    (($document.index[($id | tostring)].crate_id // -1) == 0);
  def public_path($id):
    $document.paths[($id | tostring)];

  [
    $document.paths
    | to_entries[]
    | select(.value.crate_id == 0)
    | .key
    | tonumber
  ] as $roots
| {
    seen: [],
    frontier: $roots,
  }
| until(
    (.frontier | length) == 0;
    . as $state
    | ([.seen[], .frontier[]] | unique) as $seen
    | ([
        .frontier[] as $id
        | item_refs($id)[]
        | select(local_item(.))
        | select(. as $reference | ($seen | index($reference)) == null)
      ] | unique) as $frontier
    | {
        seen: $seen,
        frontier: $frontier,
      }
  )
| ([.seen[], .frontier[]] | unique) as $reachable
| [
    $reachable[] as $source
    | item_refs($source)[] as $reference
    | public_path($reference) as $path
    | select($path != null)
    | select($path.path[0] == "tokio" or $path.path[0] == "tokio_util")
    | {
        source: (public_path($source).path // ["<unnamed public item>"] | join("::")),
        leaked: ($path.path | join("::")),
      }
  ]
| unique_by([.source, .leaked])
| .[]
| "\(.source) -> \(.leaked)"
