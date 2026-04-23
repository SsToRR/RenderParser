from __future__ import annotations

import argparse
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


BASE_URL = "https://kia.com.kz"
MODELS_URL = f"{BASE_URL}/models"
DEFAULT_INTERVAL_SECONDS = 60 * 60
DEFAULT_OUTPUT_PATH = Path(__file__).with_name("kia_models.json")

LOGGER = logging.getLogger("kia_models_parser")

PRICE_RE = re.compile(r"(?:от\s*)?[\d\s\xa0]+₸", re.IGNORECASE)
PDF_RE = re.compile(r"\.pdf(?:$|[?#])", re.IGNORECASE)
PRICE_LIST_KEYWORDS = ("прайс", "price list", "price-list", "price_list")
BROCHURE_KEYWORDS = ("брош", "brochure", "catalog", "каталог")

VOID_TAGS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}


@dataclass
class HtmlNode:
    tag: str
    attrs: dict[str, str]
    parent: HtmlNode | None = None
    children: list[HtmlNode | str] = field(default_factory=list)

    def get(self, attr_name: str, default: str = "") -> str:
        return self.attrs.get(attr_name, default)

    def classes(self) -> set[str]:
        return {item for item in self.get("class").split() if item}

    def has_class(self, class_name: str) -> bool:
        return class_name in self.classes()

    def text(self) -> str:
        parts: list[str] = []
        for child in self.children:
            if isinstance(child, str):
                parts.append(child)
            else:
                parts.append(child.text())
        return "".join(parts)

    def direct_text(self) -> str:
        return "".join(child for child in self.children if isinstance(child, str))

    def ancestors(self, include_self: bool = False) -> list[HtmlNode]:
        nodes: list[HtmlNode] = []
        current: HtmlNode | None = self if include_self else self.parent
        while current:
            nodes.append(current)
            current = current.parent
        return nodes

    def iter_nodes(self) -> Iterable[HtmlNode]:
        yield self
        for child in self.children:
            if isinstance(child, HtmlNode):
                yield from child.iter_nodes()

    def find_all(self, predicate: Callable[[HtmlNode], bool]) -> list[HtmlNode]:
        return [node for node in self.iter_nodes() if predicate(node)]

    def find_first(self, predicate: Callable[[HtmlNode], bool]) -> HtmlNode | None:
        for node in self.iter_nodes():
            if predicate(node):
                return node
        return None


class HtmlTreeBuilder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = HtmlNode("[document]", {})
        self.stack: list[HtmlNode] = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        attrs_dict = {key.lower(): value or "" for key, value in attrs if key}
        node = HtmlNode(normalized_tag, attrs_dict, parent=self.stack[-1])
        self.stack[-1].children.append(node)
        if normalized_tag not in VOID_TAGS:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        attrs_dict = {key.lower(): value or "" for key, value in attrs if key}
        self.stack[-1].children.append(HtmlNode(normalized_tag, attrs_dict, parent=self.stack[-1]))

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.lower()
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == normalized_tag:
                self.stack = self.stack[:index]
                return

    def handle_data(self, data: str) -> None:
        if data:
            self.stack[-1].children.append(data)


@dataclass
class KiaModel:
    name: str
    slug: str
    price: str
    previous_price: str
    model_url: str
    options_url: str
    price_list_url: str = ""
    brochure_url: str = ""
    errors: list[str] = field(default_factory=list)


def parse_html(html: str) -> HtmlNode:
    parser = HtmlTreeBuilder()
    parser.feed(html)
    parser.close()
    return parser.root


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def normalize_price(value: str) -> str:
    return normalize_spaces(value)


def absolute_url(href: str) -> str:
    if not href:
        return ""
    return urljoin(BASE_URL, href)


def url_path(href: str) -> str:
    return urlparse(absolute_url(href)).path


def is_model_url(href: str) -> bool:
    return re.fullmatch(r"/models/[^/]+/?", url_path(href)) is not None


def is_options_url(href: str) -> bool:
    return re.fullmatch(r"/models/[^/]+/options/?", url_path(href)) is not None


def slug_from_url(href: str) -> str:
    path_parts = [part for part in url_path(href).split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] == "models":
        return path_parts[1]
    return ""


def first_text(nodes: Iterable[HtmlNode]) -> str:
    for node in nodes:
        text = normalize_spaces(node.text())
        if text:
            return text
    return ""


def link_text(node: HtmlNode) -> str:
    return normalize_spaces(node.text())


def is_pdf_href(href: str) -> bool:
    return bool(href and PDF_RE.search(href))


def is_pdf_link(node: HtmlNode) -> bool:
    return node.tag == "a" and is_pdf_href(node.get("href"))


def contains_any_keyword(value: str, keywords: tuple[str, ...]) -> bool:
    normalized = normalize_spaces(value).lower()
    return any(keyword in normalized for keyword in keywords)


def keyword_text(node: HtmlNode) -> str:
    fields = [
        node.direct_text(),
        node.get("download"),
        node.get("title"),
        node.get("aria-label"),
        node.get("href"),
    ]
    if node.tag == "a":
        fields.append(node.text())
    return " ".join(field for field in fields if field)


def child_nodes(node: HtmlNode) -> list[HtmlNode]:
    return [child for child in node.children if isinstance(child, HtmlNode)]


def sibling_nodes(node: HtmlNode, radius: int = 2) -> list[HtmlNode]:
    if not node.parent:
        return []

    siblings = child_nodes(node.parent)
    try:
        node_index = siblings.index(node)
    except ValueError:
        return []

    start = max(0, node_index - radius)
    end = min(len(siblings), node_index + radius + 1)
    return [sibling for sibling in siblings[start:end] if sibling is not node]


def node_positions(root: HtmlNode) -> dict[int, int]:
    return {id(node): index for index, node in enumerate(root.iter_nodes())}


def tree_distance(left: HtmlNode, right: HtmlNode) -> int:
    left_path = left.ancestors(include_self=True)
    right_path = right.ancestors(include_self=True)
    right_indexes = {id(node): index for index, node in enumerate(right_path)}

    for left_index, node in enumerate(left_path):
        right_index = right_indexes.get(id(node))
        if right_index is not None:
            return left_index + right_index

    return len(left_path) + len(right_path)


def find_keyword_nodes(root: HtmlNode, keywords: tuple[str, ...]) -> list[HtmlNode]:
    nodes: list[HtmlNode] = []
    for node in root.iter_nodes():
        if contains_any_keyword(keyword_text(node), keywords):
            nodes.append(node)
    return nodes


def direct_pdf_href(node: HtmlNode) -> str:
    for candidate in node.ancestors(include_self=True):
        if is_pdf_link(candidate):
            return absolute_url(candidate.get("href"))

    descendant_link = node.find_first(is_pdf_link)
    if descendant_link:
        return absolute_url(descendant_link.get("href"))

    return ""


def find_nearest_pdf_url(
    label_node: HtmlNode,
    pdf_links: list[HtmlNode],
    positions: dict[int, int],
    used_urls: set[str],
) -> str:
    direct_href = direct_pdf_href(label_node)
    if direct_href and direct_href not in used_urls:
        return direct_href

    candidates: list[tuple[int, int, str]] = []
    label_position = positions.get(id(label_node), 0)
    for link in pdf_links:
        href = absolute_url(link.get("href"))
        if href in used_urls:
            continue

        distance = tree_distance(label_node, link)
        position_delta = abs(label_position - positions.get(id(link), label_position))
        if distance <= 8 or position_delta <= 12:
            candidates.append((distance, position_delta, href))

    if not candidates:
        return ""

    candidates.sort()
    return candidates[0][2]


def find_labeled_pdf_url(
    root: HtmlNode,
    keywords: tuple[str, ...],
    used_urls: set[str] | None = None,
) -> str:
    used_urls = used_urls or set()
    pdf_links = root.find_all(is_pdf_link)
    positions = node_positions(root)

    for label_node in find_keyword_nodes(root, keywords):
        url = find_nearest_pdf_url(label_node, pdf_links, positions, used_urls)
        if url:
            return url

    return ""


def link_context_score(link: HtmlNode, keywords: tuple[str, ...]) -> int:
    score = 0
    if contains_any_keyword(link_text(link), keywords):
        score += 100
    if contains_any_keyword(
        " ".join([link.get("download"), link.get("title"), link.get("aria-label"), link.get("href")]),
        keywords,
    ):
        score += 80

    current = link.parent
    depth = 1
    while current and current.tag != "[document]" and depth <= 4:
        if contains_any_keyword(current.direct_text(), keywords):
            score += max(0, 70 - depth * 10)

        for sibling in sibling_nodes(current):
            if contains_any_keyword(sibling.text(), keywords):
                score += max(0, 55 - depth * 10)

        current = current.parent
        depth += 1

    for sibling in sibling_nodes(link):
        if contains_any_keyword(sibling.text(), keywords):
            score += 65

    return score


def find_pdf_url_by_context(
    root: HtmlNode,
    keywords: tuple[str, ...],
    used_urls: set[str] | None = None,
) -> str:
    used_urls = used_urls or set()
    candidates: list[tuple[int, str]] = []

    for link in root.find_all(is_pdf_link):
        href = absolute_url(link.get("href"))
        if href in used_urls:
            continue

        score = link_context_score(link, keywords)
        if score:
            candidates.append((score, href))

    if not candidates:
        return ""

    candidates.sort(reverse=True)
    return candidates[0][1]


def parse_model_price(article: HtmlNode) -> str:
    price_node = article.find_first(lambda node: node.has_class("card__price"))
    if price_node:
        span_text = first_text(node for node in price_node.iter_nodes() if node.tag == "span")
        if span_text:
            return normalize_price(span_text)

        price_text = normalize_price(price_node.text())
        if price_text:
            return price_text

    article_text = normalize_spaces(article.text())
    match = PRICE_RE.search(article_text)
    return normalize_price(match.group(0)) if match else ""


def parse_previous_price(article: HtmlNode, current_price: str) -> str:
    previous_nodes = article.find_all(
        lambda node: node.tag in {"s", "del"}
        or any("old" in class_name or "previous" in class_name for class_name in node.classes())
    )
    previous_price = first_text(previous_nodes)
    if previous_price and previous_price != current_price:
        return normalize_price(previous_price)

    article_text = normalize_spaces(article.text())
    prices = [normalize_price(match.group(0)) for match in PRICE_RE.finditer(article_text)]
    for price in prices:
        if price and price != current_price:
            return price

    return ""


def find_model_link(article: HtmlNode) -> HtmlNode | None:
    name_link = article.find_first(lambda node: node.tag == "a" and node.has_class("card__name"))
    if name_link:
        return name_link

    return article.find_first(
        lambda node: node.tag == "a" and is_model_url(node.get("href")) and bool(link_text(node))
    )


def find_options_link(article: HtmlNode) -> HtmlNode | None:
    options_link = article.find_first(lambda node: node.tag == "a" and node.has_class("card__link"))
    if options_link:
        return options_link

    return article.find_first(lambda node: node.tag == "a" and is_options_url(node.get("href")))


def parse_models_page(html: str) -> list[KiaModel]:
    root = parse_html(html)
    articles = root.find_all(lambda node: node.tag == "article" and node.has_class("card"))
    models: list[KiaModel] = []
    seen_slugs: set[str] = set()

    for article in articles:
        model_link = find_model_link(article)
        if not model_link:
            continue

        model_href = model_link.get("href")
        model_url = absolute_url(model_href)
        slug = slug_from_url(model_href)
        name = link_text(model_link)

        if not slug or not name or slug in seen_slugs:
            continue

        options_link = find_options_link(article)
        options_url = absolute_url(options_link.get("href")) if options_link else urljoin(model_url, "options/")
        price = parse_model_price(article)
        previous_price = parse_previous_price(article, price)

        models.append(
            KiaModel(
                name=name,
                slug=slug,
                price=price,
                previous_price=previous_price,
                model_url=model_url,
                options_url=options_url,
            )
        )
        seen_slugs.add(slug)

    return models


def parse_document_links(html: str) -> tuple[str, str]:
    root = parse_html(html)
    used_urls: set[str] = set()

    price_list_url = find_labeled_pdf_url(root, PRICE_LIST_KEYWORDS, used_urls)
    if not price_list_url:
        price_list_url = find_pdf_url_by_context(root, PRICE_LIST_KEYWORDS, used_urls)
    if price_list_url:
        used_urls.add(price_list_url)

    brochure_url = find_labeled_pdf_url(root, BROCHURE_KEYWORDS, used_urls)
    if not brochure_url:
        brochure_url = find_pdf_url_by_context(root, BROCHURE_KEYWORDS, used_urls)

    return price_list_url, brochure_url


def fetch_text(url: str, timeout_seconds: int) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "KiaModelsParser/1.0 (+https://kia.com.kz/models)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.8",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
        return body.decode(charset, errors="replace")


def enrich_documents(model: KiaModel, timeout_seconds: int) -> None:
    try:
        options_html = fetch_text(model.options_url, timeout_seconds)
    except Exception as exc:
        model.errors.append(f"Cannot fetch options page: {exc}")
        return

    price_list_url, brochure_url = parse_document_links(options_html)
    model.price_list_url = price_list_url
    model.brochure_url = brochure_url

    if not price_list_url:
        model.errors.append("Price list PDF link was not found on options page")
    if not brochure_url:
        model.errors.append("Brochure PDF link was not found on options page")


def build_payload(models: list[KiaModel]) -> dict[str, object]:
    return {
        "source_url": MODELS_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(models),
        "models": [asdict(model) for model in models],
    }


def write_json_atomic(payload: dict[str, object], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(output_path)


def scrape_once(output_path: Path, timeout_seconds: int) -> dict[str, object]:
    models_html = fetch_text(MODELS_URL, timeout_seconds)
    models = parse_models_page(models_html)
    if not models:
        raise RuntimeError("No Kia models were parsed from the models page")

    for index, model in enumerate(models, start=1):
        LOGGER.info("Fetching documents for %s (%s/%s)", model.name, index, len(models))
        enrich_documents(model, timeout_seconds)

    payload = build_payload(models)
    write_json_atomic(payload, output_path)
    return payload


def run_loop(output_path: Path, interval_seconds: int, timeout_seconds: int) -> None:
    while True:
        started_at = time.monotonic()
        try:
            payload = scrape_once(output_path, timeout_seconds)
            LOGGER.info("Saved %s models to %s", payload["count"], output_path)
        except KeyboardInterrupt:
            raise
        except Exception:
            LOGGER.exception("Scrape cycle failed")

        elapsed_seconds = time.monotonic() - started_at
        sleep_seconds = max(0, interval_seconds - elapsed_seconds)
        LOGGER.info("Sleeping for %.0f seconds", sleep_seconds)
        time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse Kia Kazakhstan model prices and PDF links.")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"JSON output path. Defaults to {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Keep running and scrape once per interval.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Seconds between scrape cycles when --loop is set. Defaults to 3600.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds. Defaults to 30.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. Defaults to INFO.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.loop:
        run_loop(args.output, args.interval, args.timeout)
        return 0

    payload = scrape_once(args.output, args.timeout)
    print(json.dumps({"output": str(args.output), "count": payload["count"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
