import { Drawer as DrawerPrimitive } from "vaul-svelte";

import Content from "./drawer-content.svelte";
import Description from "./drawer-description.svelte";
import Footer from "./drawer-footer.svelte";
import Header from "./drawer-header.svelte";
import NestedRoot from "./drawer-nested.svelte";
import Overlay from "./drawer-overlay.svelte";
import Title from "./drawer-title.svelte";
import Root from "./drawer.svelte";

const Trigger = DrawerPrimitive.Trigger;
const Portal = DrawerPrimitive.Portal;
const Close = DrawerPrimitive.Close;

export {
  Close,
  Content,
  Description,
  //
  Root as Drawer,
  Close as DrawerClose,
  Content as DrawerContent,
  Description as DrawerDescription,
  Footer as DrawerFooter,
  Header as DrawerHeader,
  NestedRoot as DrawerNestedRoot,
  Overlay as DrawerOverlay,
  Portal as DrawerPortal,
  Title as DrawerTitle,
  Trigger as DrawerTrigger,
  Footer,
  Header,
  NestedRoot,
  Overlay,
  Portal,
  Root,
  Title,
  Trigger,
};
