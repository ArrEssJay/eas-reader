function changeAreaTextColor() {
  const areaElements = document.querySelectorAll('.area');

  const redRegex = /SASS.+/;
  const orangeRegex = /KALL1|THEB72|THEB76|OLIN2|UPWE75|KALL75|BELG1|BELG3/;

  areaElements.forEach((areaElement) => {
    const text = areaElement.textContent.trim();

    if (redRegex.test(text)) {
      areaElement.style.color = 'red';
    } else if (orangeRegex.test(text)) {
      areaElement.style.color = 'orange';
    } else {
    }
  });
}

function trimMessages() {

  const messages = document.querySelectorAll('.message', '.advice');
  messages.forEach((message) => {
    message.innerHTML = message.innerHTML
      .replaceAll("*", "")
      .trim()
  });

  const intersection = document.querySelectorAll('.message');
  intersection.forEach((message) => {
    message.innerHTML = message.innerHTML.replaceAll("//", " ⋕ ")
    const lastSlashIndex = message.innerHTML.lastIndexOf('/');
    if (lastSlashIndex !== -1) {
      // Split the string into two parts: before and after the last `/`
      let beforeSlash = message.innerHTML.slice(0, lastSlashIndex);
      let afterSlash = message.innerHTML.slice(lastSlashIndex + 1);
      
      // Recombine the before and after parts
      message.innerHTML = beforeSlash + "<br>⫲ " + afterSlash;
    }
  });

  const em = document.querySelectorAll('.agency');
  em.forEach((message) => {
    message.textContent = message.textContent
      .replaceAll("EM", "E")

  });
}

changeAreaTextColor();
trimMessages();

