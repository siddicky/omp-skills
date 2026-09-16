// No-op extension factory. omp only treats a directory as an extension root
// (and therefore discovers its sibling skills/ and agents/ capability dirs)
// when it is loadable as an extension package; this entry exists purely to
// make this directory one. It registers no commands, tools, or hooks.
export default function ompSkills(): void {}
