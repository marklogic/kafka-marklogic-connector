function insertElement(context, params, content)
{
  if (context.inputType.search('json') >= 0) {
    var result = content.toObject();
    result.newElement = "Chartreuse";
    return result;
  } else {
    /* Pass thru for non-JSON documents */
    return content;
  }
};

exports.transform = insertElement;
