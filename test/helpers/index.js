exports.collect = async function (stream) {
  const all = []
  for await (const data of stream) all.push(data)
  return all
}
