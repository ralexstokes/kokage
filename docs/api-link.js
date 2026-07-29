const apiLink = document.createElement("a");
apiLink.id = "api-reference-link";
apiLink.href = "https://stokes.io/kokage/api/";
apiLink.title = "API documentation";
apiLink.ariaLabel = "API documentation";
apiLink.textContent = "API";

document.querySelector("#mdbook-menu-bar .right-buttons")?.prepend(apiLink);
