// Similar trait to Borrow, except can return a struct with a lifetime.
pub trait AsView<'v, View: Sized + 'v> {
    fn as_view(&'v self) -> View;
}

pub trait ToOwnedView<'v>: Sized
where
    Self: 'v,
{
    type Owned: AsView<'v, Self>;

    fn to_owned_view(&'v self) -> Self::Owned;
}

impl<'v, View: 'v, Owned: AsView<'v, View>> AsView<'v, Option<View>> for Option<Owned> {
    fn as_view(&'v self) -> Option<View> {
        self.as_ref().map(|owned| owned.as_view())
    }
}

impl<'v, Owned, View> ToOwnedView<'v> for Option<View>
where
    View: ToOwnedView<'v, Owned = Owned> + 'v,
    Owned: AsView<'v, View>,
{
    type Owned = Option<Owned>;

    fn to_owned_view(&'v self) -> Self::Owned {
        self.as_ref().map(|view| view.to_owned_view())
    }
}
