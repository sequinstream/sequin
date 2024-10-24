import Content from "./card-content.svelte";
import Description from "./card-description.svelte";
import Footer from "./card-footer.svelte";
import Header from "./card-header.svelte";
import Title from "./card-title.svelte";
import Root from "./card.svelte";

export {
  //
  Root as Card,
  Content as CardContent,
  Description as CardDescription,
  Footer as CardFooter,
  Header as CardHeader,
  Title as CardTitle,
  Content,
  Description,
  Footer,
  Header,
  Root,
  Title,
};

export type HeadingLevel = "h1" | "h2" | "h3" | "h4" | "h5" | "h6";
